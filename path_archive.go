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

// pathsArchive returns the path configuration for the archive and unarchive paths
func pathsArchive(b *versionedKVBackend) []*framework.Path {
	return []*framework.Path{
		&framework.Path{
			Pattern: "archive/.*",
			Fields: map[string]*framework.FieldSchema{
				"versions": {
					Type:        framework.TypeCommaIntSlice,
					Description: "The versions to be archived. The versioned data will not be deleted, but it will no longer be returned in normal get requests.",
				},
			},
			Callbacks: map[logical.Operation]framework.OperationFunc{
				logical.UpdateOperation: b.upgradeCheck(b.pathArchiveWrite()),
				logical.CreateOperation: b.upgradeCheck(b.pathArchiveWrite()),
			},

			HelpSynopsis:    archiveHelpSyn,
			HelpDescription: archiveHelpDesc,
		},
		&framework.Path{
			Pattern: "unarchive/.*",
			Fields: map[string]*framework.FieldSchema{
				"versions": {
					Type:        framework.TypeCommaIntSlice,
					Description: "The versions to unarchive. The versions will be restored and their data will be returned on normal get requests.",
				},
			},
			Callbacks: map[logical.Operation]framework.OperationFunc{
				logical.UpdateOperation: b.upgradeCheck(b.pathUnarchiveWrite()),
				logical.CreateOperation: b.upgradeCheck(b.pathUnarchiveWrite()),
			},

			HelpSynopsis:    unarchiveHelpSyn,
			HelpDescription: unarchiveHelpDesc,
		},
	}
}

// pathUnarchiveWrite is used to unarchive a set of versions
func (b *versionedKVBackend) pathUnarchiveWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "unarchive/")

		versions := data.Get("versions").([]int)
		if len(versions) == 0 {
			return logical.ErrorResponse("No version number provided"), logical.ErrInvalidRequest
		}

		lock := locksutil.LockForKey(b.locks, key)
		lock.Lock()
		defer lock.Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		for _, verNum := range versions {
			// If there is no version or the version is destroyed continue
			lv := meta.Versions[uint64(verNum)]
			if lv == nil || lv.Destroyed {
				continue
			}

			lv.ArchiveTime = nil
		}
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		return nil, nil
	}
}

// pathArchiveWrite is used to archive a set of versions.
func (b *versionedKVBackend) pathArchiveWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "archive/")

		versions := data.Get("versions").([]int)
		if len(versions) == 0 {
			return logical.ErrorResponse("No version number provided"), logical.ErrInvalidRequest
		}

		lock := locksutil.LockForKey(b.locks, key)
		lock.Lock()
		defer lock.Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		for _, verNum := range versions {
			// If there is no latest version, or the latest version is already
			// archived or destroyed continue
			lv := meta.Versions[uint64(verNum)]
			if lv == nil || lv.Destroyed {
				continue
			}

			if lv.ArchiveTime != nil {
				archiveTime, err := ptypes.Timestamp(lv.ArchiveTime)
				if err != nil {
					return nil, err
				}

				if archiveTime.Before(time.Now()) {
					continue
				}
			}

			lv.ArchiveTime = ptypes.TimestampNow()
		}

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
