package vkv

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

		verNum := uint64(data.Get("version").(int))
		if verNum == uint64(0) {
			verNum = meta.CurrentVersion
		}

		// If there is no version with that number, return
		vm := meta.Versions[verNum]
		if vm == nil {
			return nil, nil
		}

		resp := &logical.Response{
			Data: map[string]interface{}{
				"data": nil,
				"metadata": map[string]interface{}{
					"version":      verNum,
					"created_time": ptypes.TimestampString(vm.CreatedTime),
					"archive_time": ptypes.TimestampString(vm.ArchiveTime),
					"destroyed":    vm.Destroyed,
				},
			},
		}

		// If the version has been archived return metadata with a 404
		if vm.ArchiveTime != nil {
			archiveTime, err := ptypes.Timestamp(vm.ArchiveTime)
			if err != nil {
				return nil, err
			}

			if archiveTime.Before(time.Now()) {
				return logical.RespondWithStatusCode(resp, req, http.StatusNotFound)

			}
		}

		// If the version has been destroyed return metadata with a 404
		if vm.Destroyed {
			return logical.RespondWithStatusCode(resp, req, http.StatusNotFound)

		}

		versionKey, err := b.getVersionKey(key, verNum, req.Storage)
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

		resp.Data["data"] = vData

		return resp, nil
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathDataWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := strings.TrimPrefix(req.Path, "data/")

		config, err := b.config(ctx, req.Storage)
		if err != nil {
			return nil, err
		}

		// Parse data, this can happen before the lock so we can fail early if
		// not set.
		var marshaledData []byte
		{
			dataRaw, ok := data.GetOk("data")
			if !ok {
				return logical.ErrorResponse("no data provided"), logical.ErrInvalidRequest
			}
			marshaledData, err = json.Marshal(dataRaw.(map[string]interface{}))
			if err != nil {
				return nil, err
			}
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

		// Parse options
		var versionTTL int64
		{
			optionsRaw, ok := data.GetOk("options")
			if ok {
				options := optionsRaw.(map[string]interface{})

				// Verify the CAS parameter is valid.
				cas, ok := options["cas"]
				switch {
				case !ok && config.CasRequired:
					return logical.ErrorResponse("check-and-set parameter required for this call"), logical.ErrInvalidRequest

				case !ok && meta.CasRequired:
					return logical.ErrorResponse("check-and-set parameter required for this call"), logical.ErrInvalidRequest

				case ok && uint64(cas.(float64)) != meta.CurrentVersion:
					return logical.ErrorResponse("check-and-set parameter did not match the current version"), logical.ErrInvalidRequest
				}

				// Get the TTL for this version
				versionTTLRaw, ok := options["version_ttl"]
				switch {
				case ok:
					dur, err := parseutil.ParseDurationSecond(versionTTLRaw.(string))
					if err != nil {
						return nil, err
					}
					if meta.VersionTTL < int64(dur.Seconds()) || config.VersionTTL < int64(dur.Seconds()) {
						return logical.ErrorResponse("version_ttl can not be higher than backend config or key version_ttl"), logical.ErrInvalidRequest
					}

					versionTTL = int64(dur.Seconds())

				case meta.VersionTTL > 0:
					versionTTL = meta.VersionTTL

				case config.VersionTTL > 0:
					versionTTL = config.VersionTTL
				}
			}
		}

		versionKey, err := b.getVersionKey(key, meta.CurrentVersion+1, req.Storage)
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

		versionToDelete := meta.AddVersion(version.CreatedTime, archiveTime, config.MaxVersions)
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		// Cleanup the version data that is past max version.
		if versionToDelete > 0 {
			versionKey, err := b.getVersionKey(key, versionToDelete, req.Storage)
			if err != nil {
				return nil, err
			}

			err = req.Storage.Delete(ctx, versionKey)
			if err != nil {
				// TODO: should we return this error? probs
				return nil, err
			}
		}

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

		// If there is no latest version, or the latest version is already
		// archived or destroyed return
		lv := meta.Versions[meta.CurrentVersion]
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

func (k *KeyMetadata) AddVersion(createdTime, archiveTime *timestamp.Timestamp, configMaxVersions uint32) uint64 {
	k.CurrentVersion++
	k.Versions[k.CurrentVersion] = &VersionMetadata{
		CreatedTime: createdTime,
		ArchiveTime: archiveTime,
	}

	k.UpdatedTime = createdTime
	if k.CreatedTime == nil {
		k.CreatedTime = createdTime
	}

	var maxVersions uint32
	switch {
	case k.MaxVersions != 0:
		maxVersions = k.MaxVersions
	case configMaxVersions > 0:
		maxVersions = configMaxVersions
	default:
		maxVersions = defaultMaxVersions
	}

	if uint32(k.CurrentVersion-k.OldestVersion) >= maxVersions {
		delete(k.Versions, k.OldestVersion)
		versionToDelete := k.OldestVersion
		k.OldestVersion++

		return versionToDelete
	}

	return 0
}

const dataHelpSyn = `Configures the JWT Public Key and Kubernetes API information.`
const dataHelpDesc = `
The Kubernetes Auth backend validates service account JWTs and verifies their
existence with the Kubernetes TokenReview API. This endpoint configures the
public key used to validate the JWT signature and the necessary information to
access the Kubernetes API.
`
