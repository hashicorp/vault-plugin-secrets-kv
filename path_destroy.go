package vkv

import (
	"context"
	"strings"

	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathDestroy(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "destroy/.*",
		Fields: map[string]*framework.FieldSchema{
			"version": {
				Type:        framework.TypeInt,
				Description: "",
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.UpdateOperation: b.upgradeCheck(b.pathDestroyWrite()),
			logical.CreateOperation: b.upgradeCheck(b.pathDestroyWrite()),
		},

		HelpSynopsis:    destroyHelpSyn,
		HelpDescription: destroyHelpDesc,
	}
}

func (b *versionedKVBackend) pathDestroyWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "destroy/")

		// TODO: These should be an array
		verNum := uint64(data.Get("version").(int))
		if verNum == uint64(0) {
			return logical.ErrorResponse("no version number provided"), logical.ErrInvalidRequest
		}

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		// If there is no version, or the version is already destroyed return
		lv := meta.Versions[verNum]
		if lv == nil || lv.Destroyed {
			return nil, nil
		}

		lv.Destroyed = true
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		// Delete versioned data
		versionKey, err := b.getVersionKey(key, verNum, req.Storage)
		if err != nil {
			return nil, err
		}

		err = req.Storage.Delete(ctx, versionKey)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

const destroyHelpSyn = ``
const destroyHelpDesc = `
`
