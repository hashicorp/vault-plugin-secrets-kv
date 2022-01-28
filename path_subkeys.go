package kv

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/hashicorp/vault/sdk/framework"
	"github.com/hashicorp/vault/sdk/helper/locksutil"
	"github.com/hashicorp/vault/sdk/logical"
)

func pathSubkeys(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "subkeys/" + framework.MatchAllRegex("path"),
		Fields: map[string]*framework.FieldSchema{
			"path": {
				Type:        framework.TypeString,
				Description: "Location of the secret.",
			},
			"version": {
				Type:        framework.TypeInt,
				Description: "Specifies which version to retrieve. If not provided, the current version will be used.",
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.ReadOperation: b.upgradeCheck(b.pathSubkeysRead()),
		},

		HelpSynopsis:    subkeysHelpSyn,
		HelpDescription: subkeysHelpDesc,
	}
}

// This is not a const so it can be overridden in tests
var maxSubkeysDepth = 100

// removeValues recursively walks the provided secret data represented as a
// map. All leaf nodes (i.e. empty maps and non-map values) will be replaced
// with nil in an effort to remove all values. The resulting structure will
// provide all subkeys with nesting fully intact. The modifications are made
// to the input in-place.
func removeValues(input map[string]interface{}) {
	var walk func(interface{}, int)

	walk = func(in interface{}, depth int) {
		val := reflect.ValueOf(in)

		if val.Kind() == reflect.Map {
			for _, k := range val.MapKeys() {
				v := val.MapIndex(k)
				m := in.(map[string]interface{})

				switch t := v.Interface().(type) {
				case map[string]interface{}:
					// Only continue walking if we have not reached max depth
					// and the underlying map has at least 1 key. The key is
					// otherwise treated as a leaf node and thus set to nil.
					// Setting to nil if the max depth is reached is crucial in
					// that it prevents leaking secret data as the input map is
					// being modified in-place
					if currentDepth := depth + 1; currentDepth <= maxSubkeysDepth && len(t) > 0 {
						walk(t, currentDepth)
					} else {
						m[k.String()] = nil
					}
				default:
					m[k.String()] = nil
				}
			}
		}
	}

	walk(input, 1)
}

// pathSubkeysRead handles ReadOperation requests for a specified path. Subkeys
// that exist within the entry specified by the provided path will be retrieved.
// This is done by stripping the secret data by replacing all underlying non-map
// values with null. The version parameter is used to specify which version of the
// specified secret entry to read. If not provided, the current version will be used.
func (b *versionedKVBackend) pathSubkeysRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		key := data.Get("path").(string)

		lock := locksutil.LockForKey(b.locks, key)
		lock.RLock()
		defer lock.RUnlock()

		meta, err := b.getKeyMetadata(ctx, req.Storage, key)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, nil
		}

		versionNum := meta.CurrentVersion
		versionParam := data.Get("version").(int)

		if versionParam > 0 {
			versionNum = uint64(versionParam)
		}

		versionMetadata := meta.Versions[versionNum]
		if versionMetadata == nil {
			return nil, nil
		}

		resp := &logical.Response{
			Data: map[string]interface{}{
				"subkeys": nil,
				"metadata": map[string]interface{}{
					"version":         versionNum,
					"created_time":    ptypesTimestampToString(versionMetadata.CreatedTime),
					"deletion_time":   ptypesTimestampToString(versionMetadata.DeletionTime),
					"destroyed":       versionMetadata.Destroyed,
					"custom_metadata": meta.CustomMetadata,
				},
			},
		}

		if versionMetadata.DeletionTime != nil {
			deletionTime, err := ptypes.Timestamp(versionMetadata.DeletionTime)
			if err != nil {
				return nil, err
			}

			if deletionTime.Before(time.Now()) {
				return logical.RespondWithStatusCode(resp, req, http.StatusNotFound)

			}
		}

		if versionMetadata.Destroyed {
			return logical.RespondWithStatusCode(nil, req, http.StatusNotFound)

		}

		versionKey, err := b.getVersionKey(ctx, key, versionNum, req.Storage)
		if err != nil {
			return nil, err
		}

		raw, err := req.Storage.Get(ctx, versionKey)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			return nil, errors.New("could not find version data")
		}

		version := &Version{}
		if err := proto.Unmarshal(raw.Value, version); err != nil {
			return nil, err
		}

		versionData := map[string]interface{}{}
		if err := json.Unmarshal(version.Data, &versionData); err != nil {
			return nil, err
		}

		removeValues(versionData)
		resp.Data["subkeys"] = versionData

		return resp, nil
	}
}

const subkeysHelpSyn = `Read the structure of a secret entry from the Key-Value store with the values removed.`
const subkeysHelpDesc = `
This path provides the subkeys that exist within a secret entry that exists
at the provided path. The secret entry at this path will be retrieved and
stripped of all data by replacing underlying non-map values with null.

The "version" parameter specifies which version of the secret to read when
generating the subkeys structure. If not provided, the current version will be used.
`