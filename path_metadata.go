package vkv

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathMetadata(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "metadata/.*",
		Fields: map[string]*framework.FieldSchema{
			"version": {
				Type:        framework.TypeInt,
				Description: "",
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.UpdateOperation: b.pathMetadataWrite(),
			logical.CreateOperation: b.pathMetadataWrite(),
			logical.ReadOperation:   b.pathMetadataRead(),
			logical.DeleteOperation: b.pathMetadataDelete(),
		},

		HelpSynopsis:    confHelpSyn,
		HelpDescription: confHelpDesc,
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathMetadataRead() framework.OperationFunc {
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

		versions := make(map[string]map[string]interface{}, len(meta.Versions))
		for i, v := range meta.Versions {
			versions[fmt.Sprintf("%d", i)] = map[string]interface{}{
				"created_time": ptypes.TimestampString(v.CreatedTime),
				"archive_time": ptypes.TimestampString(v.ArchiveTime),
				"destroyed":    v.Destroyed,
			}
		}

		return &logical.Response{
			Data: map[string]interface{}{
				"versions":        versions,
				"current_version": meta.CurrentVersion,
				"oldest_version":  meta.OldestVersion,
				"created_time":    ptypes.TimestampString(meta.CreatedTime),
				"updated_time":    ptypes.TimestampString(meta.UpdatedTime),
				"version_ttl":     meta.Version_TTL,
				"max_versions":    meta.MaxVersions,
			},
		}, nil
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathMetadataWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		panic(key)

		return nil, nil

	}
}

func (b *versionedKVBackend) pathMetadataDelete() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		panic(key)

		return nil, nil
	}
}

const metadataHelpSyn = `Configures the JWT Public Key and Kubernetes API information.`
const metadataHelpDesc = `
The Kubernetes Auth backend validates service account JWTs and verifies their
existence with the Kubernetes TokenReview API. This endpoint configures the
public key used to validate the JWT signature and the necessary information to
access the Kubernetes API.
`
