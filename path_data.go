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
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
	"github.com/mitchellh/mapstructure"
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
			logical.UpdateOperation: b.upgradeCheck(b.pathDataWrite()),
			logical.CreateOperation: b.upgradeCheck(b.pathDataWrite()),
			logical.ReadOperation:   b.upgradeCheck(b.pathDataRead()),
			logical.DeleteOperation: b.upgradeCheck(b.pathDataDelete()),
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

		var verNum uint64
		verRaw, ok := data.GetOk("version")
		if ok {
			verNum = uint64(verRaw.(int))
		} else {
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
					"created_time": ptypesTimestampToString(vm.CreatedTime),
					"archive_time": ptypesTimestampToString(vm.ArchiveTime),
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
		{
			var casRaw interface{}
			var casOk bool
			optionsRaw, ok := data.GetOk("options")
			if ok {
				options := optionsRaw.(map[string]interface{})

				// Verify the CAS parameter is valid.
				casRaw, casOk = options["cas"]
			}

			switch {
			case !casOk && config.CasRequired:
				return logical.ErrorResponse("check-and-set parameter required for this call"), logical.ErrInvalidRequest

			case !casOk && meta.CasRequired:
				return logical.ErrorResponse("check-and-set parameter required for this call"), logical.ErrInvalidRequest

			case casOk:
				var cas int
				if err := mapstructure.WeakDecode(casRaw, &cas); err != nil {
					return logical.ErrorResponse("error parsing check-and-set parameter"), logical.ErrInvalidRequest
				}
				if uint64(cas) != meta.CurrentVersion {
					return logical.ErrorResponse("check-and-set parameter did not match the current version"), logical.ErrInvalidRequest
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

		versionToDelete := meta.AddVersion(version.CreatedTime, nil, config.MaxVersions)
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		// Cleanup the version data that is past max version.
		if versionToDelete > 0 {

			// Create a list of version keys to delete. We will delete from the
			// back of the array first so we can delete the oldest versions
			// first. If there is an error deleting one of the keys we can
			// ensure the rest will be deleted on the next go around.
			var versionKeysToDelete []string

			versionKey, err := b.getVersionKey(key, versionToDelete, req.Storage)
			if err != nil {
				return nil, err
			}
			versionKeysToDelete = append(versionKeysToDelete, versionKey)

			for i := versionToDelete; i > 0; i-- {
				versionKey, err := b.getVersionKey(key, i, req.Storage)
				if err != nil {
					return nil, err
				}

				// We intentionally do not return these errors here. If the get
				// or delete fail they will be cleaned up on the next write.
				v, _ := req.Storage.Get(ctx, versionKey)
				if v == nil {
					break
				}
			}

			for i := len(versionKeysToDelete) - 1; i >= 0; i-- {
				req.Storage.Delete(ctx, versionKeysToDelete[i])
				// TODO: should we return this error? probs
			}

		}

		return &logical.Response{
			Data: map[string]interface{}{
				"version": meta.CurrentVersion,
				//TODO: SHould this be omited?
				"archive_time": ptypesTimestampToString(nil),
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
	if k.Versions == nil {
		k.Versions = map[uint64]*VersionMetadata{}
	}

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
		versionToDelete := k.CurrentVersion - uint64(maxVersions)
		// We need to do a loop here in the event that the max versions has
		// changed and we need to delete more than one entry.
		for i := k.OldestVersion; i < versionToDelete+1; i++ {
			delete(k.Versions, i)
		}

		k.OldestVersion = versionToDelete + 1

		return versionToDelete
	}

	return 0
}

const dataHelpSyn = ``
const dataHelpDesc = `
`
