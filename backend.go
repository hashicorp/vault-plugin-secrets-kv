package vkv

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/vault/helper/jsonutil"
	"github.com/hashicorp/vault/helper/keysutil"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/helper/salt"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
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
	salt      *salt.Salt
	l         sync.RWMutex
	locks     []*locksutil.LockEntry
}

func Factory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	versioned := conf.Config["versioned"]
	versioned = "true"

	var b logical.Backend
	var err error
	switch versioned {
	case "false", "":
		return LeaseSwitchedPassthroughBackend(ctx, conf, conf.Config["leased_passthrough"] == "true")
	case "true":
		b, err = VersionedKVFactory(ctx, conf)
	}
	if err != nil {
		return nil, err
	}

	if _, ok := conf.Config["upgrade"]; ok {
		//err := b.Upgrade(ctx, conf.StorageView)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Factory returns a new backend as logical.Backend.
func VersionedKVFactory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	b := &versionedKVBackend{}

	b.Backend = &framework.Backend{
		BackendType: logical.TypeLogical,
		Help:        backendHelp,

		PathsSpecial: &logical.Paths{
			SealWrapStorage: []string{
				"/",
			},
		},

		Paths: framework.PathAppend(
			[]*framework.Path{
				pathConfig(b),
				pathData(b),
				pathMetadata(b),
				pathDestroy(b),
			},
			pathsArchive(b),
		),
	}

	b.locks = locksutil.CreateLocks()

	if err := b.Setup(ctx, conf); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *versionedKVBackend) Invalidate(ctx context.Context, key string) {
	switch key {
	case salt.DefaultLocation:
		b.l.Lock()
		b.salt = nil
		b.l.Unlock()
	case "policy/metadata":

	}
}

func (b *versionedKVBackend) Salt(s logical.Storage) (*salt.Salt, error) {
	b.l.RLock()
	if b.salt != nil {
		defer b.l.RUnlock()
		return b.salt, nil
	}
	b.l.RUnlock()
	b.l.Lock()
	defer b.l.Unlock()
	if b.salt != nil {
		return b.salt, nil
	}
	salt, err := salt.NewSalt(s, &salt.Config{
		HashFunc: salt.SHA256Hash,
		Location: salt.DefaultLocation,
	})
	if err != nil {
		return nil, err
	}
	b.salt = salt
	return salt, nil
}

func (b *versionedKVBackend) Policy(ctx context.Context, s logical.Storage) (*keysutil.Policy, error) {
	b.l.RLock()
	if b.keyPolicy != nil {
		defer b.l.RUnlock()
		return b.keyPolicy, nil
	}
	b.l.RUnlock()
	b.l.Lock()
	defer b.l.Unlock()

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

func (b *versionedKVBackend) getVersionKey(key string, version uint64, s logical.Storage) (string, error) {
	salt, err := b.Salt(s)
	if err != nil {
		return "", err
	}

	salted := salt.SaltID(fmt.Sprintf("%s|%d", key, version))

	return fmt.Sprintf("versions/%s/%s", salted[0:3], salted[3:]), nil
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
