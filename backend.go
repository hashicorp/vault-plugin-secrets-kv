package vkv

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/vault/helper/jsonutil"
	"github.com/hashicorp/vault/helper/keysutil"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
	log "github.com/mgutz/logxi/v1"
)

const (
	configPath         string = "config"
	rolePrefix         string = "role/"
	defaultMaxVersions uint32 = 10
)

// versionedKVBackend implements logical.Backend
type versionedKVBackend struct {
	*framework.Backend

	keyPolicy *keysutil.Policy
	l         sync.RWMutex
	locks     []*locksutil.LockEntry
}

// Factory returns a new backend as logical.Backend.
func Factory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	b, err := Backend(conf.StorageView, conf.Logger)
	if err != nil {
		return nil, err
	}

	if err := b.Setup(ctx, conf); err != nil {
		return nil, err
	}
	return b, nil
}

func Backend(s logical.Storage, logger log.Logger) (*versionedKVBackend, error) {
	b := &versionedKVBackend{}

	b.Backend = &framework.Backend{
		BackendType: logical.TypeLogical,
		Help:        backendHelp,
		Paths: framework.PathAppend(
			[]*framework.Path{
				pathConfig(b),
				pathData(b),
				pathMetadata(b),
			},
		),
	}

	b.locks = locksutil.CreateLocks()

	return b, nil
}

func (b *versionedKVBackend) Policy(ctx context.Context, s logical.Storage) (*keysutil.Policy, error) {
	if b.keyPolicy != nil {
		return b.keyPolicy, nil
	}

	// Check if the policy already exists
	raw, err := s.Get(ctx, "policy/metadata")
	if err != nil {
		return nil, err
	}
	if raw != nil {
		// Decode the policy
		var policy keysutil.Policy
		err = jsonutil.DecodeJSON(raw.Value, &policy)
		if err != nil {
			return nil, err
		}

		b.keyPolicy = &policy
		return b.keyPolicy, nil
	}

	// Policy didn't exist, create it.
	policy := &keysutil.Policy{
		Name:                 "metadata",
		Type:                 keysutil.KeyType_AES256_GCM96,
		Derived:              true,
		KDF:                  keysutil.Kdf_hkdf_sha256,
		ConvergentEncryption: true,
		ConvergentVersion:    2,
	}

	err = policy.Rotate(ctx, s)
	if err != nil {
		return nil, err
	}

	b.keyPolicy = policy
	return b.keyPolicy, nil
}

// config takes a storage object and returns a kubeConfig object
func (b *versionedKVBackend) config(ctx context.Context, s logical.Storage) (*Configuration, error) {
	raw, err := s.Get(ctx, configPath)
	if err != nil {
		return nil, err
	}

	conf := &Configuration{}
	if raw == nil {
		return conf, nil
	}

	if err := json.Unmarshal(raw.Value, conf); err != nil {
		return nil, err
	}

	return conf, nil
}

func (b *versionedKVBackend) getVersionKey(key string, version uint64) (string, error) {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s%d", key, version)))

	str := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return fmt.Sprintf("versions/%s/%s/%s", str[0], str[1], str[2:]), nil
}

func (b *versionedKVBackend) getKeyMetadata(ctx context.Context, s logical.Storage, key string) (*KeyMetadata, error) {
	policy, err := b.Policy(ctx, s)
	if err != nil {
		return nil, err
	}

	es, err := NewEncryptedKeyStorage(EncryptedKeyStorageConfig{
		Storage: s,
		Policy:  policy,
		Prefix:  metadataPrefix,
	})
	if err != nil {
		return nil, err
	}

	item, err := es.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	meta := &KeyMetadata{}
	err = proto.Unmarshal(item.Value, meta)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key metadata from storage: %v", err)
	}

	return meta, nil
}

func (b *versionedKVBackend) writeKeyMetadata(ctx context.Context, s logical.Storage, meta *KeyMetadata) error {
	policy, err := b.Policy(ctx, s)
	if err != nil {
		return err
	}

	es, err := NewEncryptedKeyStorage(EncryptedKeyStorageConfig{
		Storage: s,
		Policy:  policy,
		Prefix:  metadataPrefix,
	})
	if err != nil {
		return err
	}

	bytes, err := proto.Marshal(meta)
	if err != nil {
		return err
	}

	err = es.Put(ctx, &logical.StorageEntry{
		Key:   meta.Key,
		Value: bytes,
	})
	if err != nil {
		return err
	}

	return nil
}

var backendHelp string = `
`
