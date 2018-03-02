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
			"versions": {
				Type:        framework.TypeCommaIntSlice,
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

		versions := data.Get("versions").([]int)
		if len(versions) == 0 {
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

		for _, verNum := range versions {
			// If there is no version, or the version is already destroyed,
			// continue
			lv := meta.Versions[uint64(verNum)]
			if lv == nil || lv.Destroyed {
				continue
			}

			lv.Destroyed = true
		}

		// write the metadata key before deleting the versions
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		for _, verNum := range versions {
			// Delete versioned data
			versionKey, err := b.getVersionKey(key, uint64(verNum), req.Storage)
			if err != nil {
				return nil, err
			}

			err = req.Storage.Delete(ctx, versionKey)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	}
}

const destroyHelpSyn = ``
const destroyHelpDesc = `
`
