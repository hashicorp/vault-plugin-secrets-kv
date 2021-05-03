package kv

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/vault/sdk/framework"
	"github.com/hashicorp/vault/sdk/helper/locksutil"
	"github.com/hashicorp/vault/sdk/logical"
	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
)

func pathExpanded(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "expanded/(?P<path>.*?)/jsonpath/(?P<jsonpath>.*)",
		Fields: map[string]*framework.FieldSchema{
			"path": {
				Type:        framework.TypeString,
				Description: "Location of the secret.",
			},
			"jsonpath": {
				Type:        framework.TypeString,
				Description: "Location within the secret.",
			},
			"version": {
				Type:        framework.TypeInt,
				Description: "If provided during a read, the value at the version number will be returned",
			},
			"data": {
				Type: framework.TypeAny,
			},
		},
		Operations: map[logical.Operation]framework.OperationHandler{
			logical.ReadOperation: &framework.PathOperation{
				Callback: b.pathExpandedRead(),
				Summary:  "read",
			},
			logical.UpdateOperation: &framework.PathOperation{
				Callback: b.pathExpandedWrite(),
				Summary:  "write",
			},
		},
		//	//logical.CreateOperation: b.pathExpandedWrite(),
		//	//logical.DeleteOperation: b.pathExpandedDelete(),
		//	//logical.ListOperation:   b.pathExpandedList(),
		//},

		//ExistenceCheck: b.metadataExistenceCheck(),

		//HelpSynopsis:    confHelpSyn,
		//HelpDescription: confHelpDesc,
	}
}

func (b *versionedKVBackend) pathExpandedRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := data.Get("path").(string)
		verParam := data.Get("version").(int)
		jsonpath := strings.ReplaceAll(data.Get("jsonpath").(string), "/", ".")

		j, err := jp.ParseString(jsonpath)
		if err != nil {
			return nil, err
		}

		meta, b, err := b.readData(ctx, req.Storage, key, verParam)
		resp := &logical.Response{
			Data: map[string]interface{}{
				"metadata": meta,
			},
		}

		switch {
		case err != nil:
			return nil, err
		case meta == nil:
			return nil, nil
		case b == nil:
			return logical.RespondWithStatusCode(resp, req, http.StatusNotFound)
		}

		obj, err := oj.Parse(b)
		if err != nil {
			return nil, err
		}
		got := j.Get(obj)
		resp.Data["data"] = nil
		if len(got) > 0 {
			resp.Data["data"] = got[0]
		}

		return resp, nil
	}
}

func (b *versionedKVBackend) pathExpandedWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := data.Get("path").(string)
		val := data.Get("data").(string)
		jsonpath := strings.ReplaceAll(data.Get("jsonpath").(string), "/", ".")

		j, err := jp.ParseString(jsonpath)
		if err != nil {
			return nil, err
		}

		config, err := b.config(ctx, req.Storage)
		if err != nil {
			return nil, err
		}

		lock := locksutil.LockForKey(b.locks, key)
		lock.Lock()
		defer lock.Unlock()

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

		versionKey, err := b.getVersionKey(ctx, key, meta.CurrentVersion, req.Storage)
		if err != nil {
			return nil, err
		}

		entry, err := req.Storage.Get(ctx, versionKey)
		if err != nil {
			return nil, err
		}
		version := &Version{}
		var obj interface{}
		if entry != nil {
			if err := proto.Unmarshal(entry.Value, version); err != nil {
				return nil, err
			}
			obj, err = oj.Parse(version.Data)
			if err != nil {
				return nil, err
			}
		} else {
			version.CreatedTime = ptypes.TimestampNow()
			var bld oj.Builder
			_ = bld.Object()
			obj = bld.Result()
		}

		err = j.Set(obj, val)
		if err != nil {
			return nil, err
		}
		marshaledData := oj.JSON(obj)

		// Create a version key for the new version
		versionKey, err = b.getVersionKey(ctx, key, meta.CurrentVersion+1, req.Storage)
		if err != nil {
			return nil, err
		}
		version.Data = []byte(marshaledData)

		ctime, err := ptypes.Timestamp(version.CreatedTime)
		if err != nil {
			return logical.ErrorResponse("unexpected error converting %T(%v) to time.Time: %v", version.CreatedTime, version.CreatedTime, err), logical.ErrInvalidRequest
		}

		if !config.IsDeleteVersionAfterDisabled() {
			if dtime, ok := deletionTime(ctime, deleteVersionAfter(config), deleteVersionAfter(meta)); ok {
				dt, err := ptypes.TimestampProto(dtime)
				if err != nil {
					return logical.ErrorResponse("error setting deletion_time: converting %v to protobuf: %v", dtime, err), logical.ErrInvalidRequest
				}
				version.DeletionTime = dt
			}
		}

		buf, err := proto.Marshal(version)
		if err != nil {
			return nil, err
		}

		// Write the new version
		if err := req.Storage.Put(ctx, &logical.StorageEntry{
			Key:   versionKey,
			Value: buf,
		}); err != nil {
			return nil, err
		}

		vm, _ := meta.AddVersion(version.CreatedTime, version.DeletionTime, config.MaxVersions)
		err = b.writeKeyMetadata(ctx, req.Storage, meta)
		if err != nil {
			return nil, err
		}

		// We create the response here so we can add warnings to it below.
		resp := &logical.Response{
			Data: map[string]interface{}{
				"version":       meta.CurrentVersion,
				"created_time":  ptypesTimestampToString(vm.CreatedTime),
				"deletion_time": ptypesTimestampToString(vm.DeletionTime),
				"destroyed":     vm.Destroyed,
			},
		}

		return resp, nil
	}
}
