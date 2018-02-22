package vkv

import (
	"context"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathsArchive(b *versionedKVBackend) []*framework.Path {
	return []*framework.Path{
		&framework.Path{
			Pattern: "archive/.*",
			Fields: map[string]*framework.FieldSchema{
				"version": {
					Type:        framework.TypeInt,
					Description: "",
				},
			},
			Callbacks: map[logical.Operation]framework.OperationFunc{
				logical.UpdateOperation: b.pathDataWrite(),
				logical.CreateOperation: b.pathArchiveWrite(),
			},

			HelpSynopsis:    archiveHelpSyn,
			HelpDescription: archiveHelpDesc,
		},
		&framework.Path{
			Pattern: "unarchive/.*",
			Fields: map[string]*framework.FieldSchema{
				"version": {
					Type:        framework.TypeInt,
					Description: "",
				},
			},
			Callbacks: map[logical.Operation]framework.OperationFunc{
				logical.UpdateOperation: b.pathUnarchiveWrite(),
				logical.CreateOperation: b.pathUnarchiveWrite(),
			},

			HelpSynopsis:    unarchiveHelpSyn,
			HelpDescription: unarchiveHelpDesc,
		},
	}
}

func (b *versionedKVBackend) pathUnarchiveWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "unarchive/")

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

		// If there is no version, or the version is already archived or
		// destroyed return
		lv := meta.Versions[verNum]
		if lv == nil || lv.Destroyed {
			return nil, nil
		}

		lv.ArchiveTime = nil
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

func (b *versionedKVBackend) pathArchiveWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "archive/")

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

		// If there is no latest version, or the latest version is already
		// archived or destroyed return
		lv := meta.Versions[verNum]
		if lv == nil || lv.Destroyed {
			return nil, nil
		}

		if lv.ArchiveTime != nil {
			archiveTime, err := ptypes.Timestamp(lv.ArchiveTime)
			if err != nil {
				return nil, err
			}

			if archiveTime.Before(time.Now()) {
				return nil, nil
			}
		}

		lv.ArchiveTime = ptypes.TimestampNow()

		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

const archiveHelpSyn = ``
const archiveHelpDesc = `
`

const unarchiveHelpSyn = ``
const unarchiveHelpDesc = `
`
