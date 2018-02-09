package vkv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/helper/parseutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

const (
	metadataPrefix string = "metadata/"
	versionPrefix  string = "versions/"
)

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathData(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "data/.*",
		Fields: map[string]*framework.FieldSchema{
			"version": {
				Type:        framework.TypeInt,
				Description: "",
			},
			"options": {
				Type:        framework.TypeMap,
				Description: "",
			},
			"data": {
				Type:        framework.TypeMap,
				Description: "",
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.UpdateOperation: b.pathDataWrite(),
			logical.CreateOperation: b.pathDataWrite(),
			logical.ReadOperation:   b.pathDataRead(),
			logical.DeleteOperation: b.pathDataDelete(),
		},

		HelpSynopsis:    confHelpSyn,
		HelpDescription: confHelpDesc,
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathDataRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		var verNum uint64 = 0
		// If there is no latest version, or the latest version is already a
		// deletion marker return
		vm := meta.LatestVersionMeta()
		if verNum > 0 {
			vm = meta.Versions[verNum]
		}
		if vm == nil {
			return nil, nil
		}

		if vm.ArchiveTime != nil {
			archiveTime, err := ptypes.Timestamp(vm.ArchiveTime)
			if err != nil {
				return nil, err
			}

			if archiveTime.Before(time.Now()) {
				return nil, nil
			}
		}

		if vm.Destroyed {
			return nil, nil
		}

		versionKey, err := b.getVersionKey(key, meta.CurrentVersion)
		if err != nil {
			return nil, err
		}

		raw, err := req.Storage.Get(ctx, versionKey)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, errors.New("could not find version")
		}

		version := &Version{}
		if err := proto.Unmarshal(raw.Value, version); err != nil {
			return nil, err
		}

		vData := map[string]interface{}{}
		if err := json.Unmarshal(version.Data, &vData); err != nil {
			return nil, err
		}

		return &logical.Response{
			Data: vData,
		}, nil
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathDataWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

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

		var versionTTL int64

		optionsRaw, ok := data.GetOk("options")
		if ok {
			options := optionsRaw.(map[string]interface{})

			cas, ok := options["cas"]
			if ok && uint64(cas.(float64)) != meta.CurrentVersion {
				return logical.ErrorResponse("check-and-set parameter did not match the current version"), logical.ErrInvalidRequest
			}

			versionTTLRaw, ok := options["version_ttl"]
			if ok {
				dur, err := parseutil.ParseDurationSecond(versionTTLRaw.(string))
				if err != nil {
					return nil, err
				}
				versionTTL = int64(dur.Seconds())
			}
		}

		dataRaw, ok := data.GetOk("data")
		if !ok {
			return logical.ErrorResponse("no data provided"), logical.ErrInvalidRequest
		}
		marshaledData, err := json.Marshal(dataRaw.(map[string]interface{}))
		if err != nil {
			return nil, err
		}

		versionKey, err := b.getVersionKey(key, meta.CurrentVersion+1)
		if err != nil {
			return nil, err
		}
		version := &Version{
			Data:        marshaledData,
			CreatedTime: ptypes.TimestampNow(),
		}

		buf, err := proto.Marshal(version)
		if err != nil {
			return nil, err
		}

		if err := req.Storage.Put(ctx, &logical.StorageEntry{
			Key:   versionKey,
			Value: buf,
		}); err != nil {
			return nil, err
		}

		var archiveTime *timestamp.Timestamp
		if versionTTL > 0 {
			archiveTime = &timestamp.Timestamp{}
			*archiveTime = *(version.CreatedTime)
			archiveTime.Seconds += versionTTL
		}

		meta.AddVersion(version.CreatedTime, archiveTime)

		b.writeKeyMetadata(ctx, req.Storage, meta)

		return &logical.Response{
			Data: map[string]interface{}{
				"version":      meta.CurrentVersion,
				"archive_time": ptypes.TimestampString(archiveTime),
			},
		}, nil
	}
}

func (b *versionedKVBackend) pathDataDelete() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

		locksutil.LockForKey(b.locks, key).Lock()
		defer locksutil.LockForKey(b.locks, key).Unlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		// If there is no latest version, or the latest version is already a
		// deletion marker return
		lv := meta.LatestVersionMeta()
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

func (k *KeyMetadata) LatestVersionMeta() *VersionMetadata {
	if len(k.Versions) == 0 {
		return nil
	}

	return k.Versions[k.CurrentVersion]
}

func (k *KeyMetadata) AddVersion(createdTime, archiveTime *timestamp.Timestamp) {
	k.CurrentVersion++
	k.Versions[k.CurrentVersion] = &VersionMetadata{
		CreatedTime: createdTime,
		ArchiveTime: archiveTime,
	}

	k.UpdatedTime = createdTime
	if k.CreatedTime == nil {
		k.CreatedTime = createdTime
	}
}

const dataHelpSyn = `Configures the JWT Public Key and Kubernetes API information.`
const dataHelpDesc = `
The Kubernetes Auth backend validates service account JWTs and verifies their
existence with the Kubernetes TokenReview API. This endpoint configures the
public key used to validate the JWT signature and the necessary information to
access the Kubernetes API.
`
