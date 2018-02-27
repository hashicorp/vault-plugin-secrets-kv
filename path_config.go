package vkv

import (
	"context"

	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

const warningACLReadAccess string = "Read access to this endpoint should be controlled via ACLs as it will return the configuration information as-is, including any passwords."

// pathConfig returns the path configuration for CRUD operations on the backend
// configuration.
func pathConfig(b *versionedKVBackend) *framework.Path {
	return &framework.Path{
		Pattern: "config$",
		Fields: map[string]*framework.FieldSchema{
			"max_versions": {
				Type:        framework.TypeInt,
				Description: "",
			},
		},
		Callbacks: map[logical.Operation]framework.OperationFunc{
			logical.UpdateOperation: b.pathConfigWrite(),
			logical.CreateOperation: b.pathConfigWrite(),
			logical.ReadOperation:   b.pathConfigRead(),
		},

		HelpSynopsis:    confHelpSyn,
		HelpDescription: confHelpDesc,
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathConfigRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		if config, err := b.config(ctx, req.Storage); err != nil {
			return nil, err
		} else if config == nil {
			return nil, nil
		} else {
			// Create a map of data to be returned
			resp := &logical.Response{
				Data: map[string]interface{}{
					"max_versions": config.MaxVersions,
				},
			}

			return resp, nil
		}
	}
}

// pathConfigWrite handles create and update commands to the config
func (b *versionedKVBackend) pathConfigWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		maxVersions := data.Get("max_versions").(uint32)

		config := &Configuration{
			MaxVersions: maxVersions,
		}

		// TODO: store proto message instead of json
		entry, err := logical.StorageEntryJSON(configPath, config)
		if err != nil {
			return nil, err
		}

		if err := req.Storage.Put(ctx, entry); err != nil {
			return nil, err
		}
		return nil, nil
	}
}

const confHelpSyn = ``
const confHelpDesc = `
`
