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
			"ttl": {
				Type:        framework.TypeString,
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
					"ttl":          config.VersionTTL,
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

		ttl := data.Get("ttl").(int64)

		config := &Configuration{
			VersionTTL:  ttl,
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

const confHelpSyn = `Configures the JWT Public Key and Kubernetes API information.`
const confHelpDesc = `
The Kubernetes Auth backend validates service account JWTs and verifies their
existence with the Kubernetes TokenReview API. This endpoint configures the
public key used to validate the JWT signature and the necessary information to
access the Kubernetes API.
`
