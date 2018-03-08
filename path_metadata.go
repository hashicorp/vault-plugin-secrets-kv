package vkv

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

func ptypesTimestampToString(t *timestamp.Timestamp) string {
	if t == nil {
		return ""
	}

	return ptypes.TimestampString(t)
}

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathMetadata(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "metadata/.*",
		Fields: map[string]*framework.FieldSchema{
			"cas_required": {
				Type: framework.TypeBool,
				Description: `
If true the key will require the cas parameter to be set on all write requests.
If false, the backend’s configuration will be used.`,
			},
			"max_versions": {
				Type: framework.TypeInt,
				Description: `
The number of versions to keep. If not set, the backend’s configured max
version is used.`,
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.UpdateOperation: b.upgradeCheck(b.pathMetadataWrite()),
			logical.CreateOperation: b.upgradeCheck(b.pathMetadataWrite()),
			logical.ReadOperation:   b.upgradeCheck(b.pathMetadataRead()),
			logical.DeleteOperation: b.upgradeCheck(b.pathMetadataDelete()),
			logical.ListOperation:   b.upgradeCheck(b.pathMetadataList()),
		},

		ExistenceCheck: b.metadataExistenceCheck(),

		HelpSynopsis:    confHelpSyn,
		HelpDescription: confHelpDesc,
	}
}

func (b *versionedKVBackend) metadataExistenceCheck() framework.ExistenceFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (bool, error) {
		key := strings.TrimPrefix(req.Path, "metadata/")

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return false, err
		}

		return meta != nil, nil
	}
}

func (b *versionedKVBackend) pathMetadataList() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "metadata/")

		// Get an encrypted key storage object
		policy, err := b.Policy(ctx, req.Storage)
		if err != nil {
			return nil, err
		}

		es, err := NewEncryptedKeyStorage(EncryptedKeyStorageConfig{
			Storage: req.Storage,
			Policy:  policy,
			Prefix:  metadataPrefix,
		})
		if err != nil {
			return nil, err
		}

		// Use encrypted key storage to list the keys
		keys, err := es.List(ctx, key)
		return logical.ListResponse(keys), err
	}
}

func (b *versionedKVBackend) pathMetadataRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "metadata/")

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		versions := make(map[string]interface{}, len(meta.Versions))
		for i, v := range meta.Versions {
			versions[fmt.Sprintf("%d", i)] = map[string]interface{}{
				"created_time": ptypesTimestampToString(v.CreatedTime),
				"archive_time": ptypesTimestampToString(v.ArchiveTime),
				"destroyed":    v.Destroyed,
			}
		}

		return &logical.Response{
			Data: map[string]interface{}{
				"versions":        versions,
				"current_version": meta.CurrentVersion,
				"oldest_version":  meta.OldestVersion,
				"created_time":    ptypesTimestampToString(meta.CreatedTime),
				"updated_time":    ptypesTimestampToString(meta.UpdatedTime),
				"max_versions":    meta.MaxVersions,
			},
		}, nil
	}
}

func (b *versionedKVBackend) pathMetadataWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "metadata/")

		maxRaw, mOk := data.GetOk("max_versions")
		casRaw, cOk := data.GetOk("cas_required")

		// Fast path validation
		if !mOk && !cOk {
			return nil, nil
		}

		config, err := b.config(ctx, req.Storage)
		if err != nil {
			return nil, err
		}

		if cOk && config.CasRequired && !casRaw.(bool) {
			return logical.ErrorResponse("Can not set cas_required to false if mandated by backend config"), logical.ErrInvalidRequest
		}

		if mOk && config.MaxVersions > 0 && config.MaxVersions < uint32(maxRaw.(int)) {
			return logical.ErrorResponse("Can not set max_versions higher than backend config setting"), logical.ErrInvalidRequest
		}

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			meta = &KeyMetadata{
				Key:      key,
				Versions: map[uint64]*VersionMetadata{},
			}
		}

		if mOk {
			meta.MaxVersions = uint32(maxRaw.(int))
		}
		if cOk {
			meta.CasRequired = casRaw.(bool)
		}

		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		return nil, err
	}
}

func (b *versionedKVBackend) pathMetadataDelete() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "metadata/")

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		// Delete each version.
		for id, _ := range meta.Versions {
			versionKey, err := b.getVersionKey(key, id, req.Storage)
			if err != nil {
				return nil, err
			}

			// TODO: multierror?
			err = req.Storage.Delete(ctx, versionKey)
			if err != nil {
				return nil, err
			}
		}

		// Get an encrypted key storage object
		policy, err := b.Policy(ctx, req.Storage)
		if err != nil {
			return nil, err
		}

		es, err := NewEncryptedKeyStorage(EncryptedKeyStorageConfig{
			Storage: req.Storage,
			Policy:  policy,
			Prefix:  path.Join(b.storagePrefix, metadataPrefix),
		})
		if err != nil {
			return nil, err
		}

		// Use encrypted key storage to delete the key
		err = es.Delete(ctx, key)
		return nil, err
	}
}

const metadataHelpSyn = ``
const metadataHelpDesc = `

`
