package vkv

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hashicorp/vault/helper/keysutil"
	"github.com/hashicorp/vault/helper/locksutil"
	"github.com/hashicorp/vault/helper/salt"
	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

const (
	// configPath is the location where the config is stored
	configPath string = "config"

	// metadataPrefix is the prefix where the key metadata is stored.
	metadataPrefix string = "metadata/"

	// versionPrefix is the prefix where the version data is stored.
	versionPrefix string = "versions/"

	// defaultMaxVersions is the number of versions to keep around unless set by
	// the config or key configuration.
	defaultMaxVersions uint32 = 10
)

// versionedKVBackend implements logical.Backend
type versionedKVBackend struct {
	*framework.Backend

	// keyPolicy is a cached version of the policy used to encrypt the storage
	// keys for the key metadata objects.
	keyPolicy *keysutil.Policy

	// salt is the cached version of the salt used to create paths for version
	// data storage paths.
	salt *salt.Salt

	// l locks the keyPolicy and salt caches.
	l sync.RWMutex

	// locks is a slice of 256 locks that are used to protect key and version
	// updates.
	locks []*locksutil.LockEntry

	// storagePrefix is the prefix given to all the data for a versioned KV
	// store. We prefix this data so that upgrading from a passthrough backend
	// to a versioned backend is easier. This value is passed from Vault core
	// through the backend config.
	storagePrefix string

	// upgrading is an atomic value denoting if the backend is in the process of
	// upgrading its data.
	upgrading *uint32
}

// Factory will return a logical backend of type versionedKVBackend or
// PassthroughBackend based on the config passed in.
func Factory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	versioned := conf.Config["versioned"]

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

	return b, nil
}

// Factory returns a new backend as logical.Backend.
func VersionedKVFactory(ctx context.Context, conf *logical.BackendConfig) (logical.Backend, error) {
	b := &versionedKVBackend{
		upgrading: new(uint32),
	}

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

	if conf.Config["UID"] == "" {
		return nil, errors.New("could not initialize versioned K/V Store, no UID was provided")
	}
	b.storagePrefix = conf.Config["UID"]

	if err := b.Setup(ctx, conf); err != nil {
		return nil, err
	}

	if _, ok := conf.Config["upgrade"]; ok {
		err := b.Upgrade(ctx, conf.StorageView)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

// Invalidate invalidates the salt and the policy so replication secondaries can
// cache these values.
func (b *versionedKVBackend) Invalidate(ctx context.Context, key string) {
	switch key {
	case path.Join(b.storagePrefix, salt.DefaultLocation):
		b.l.Lock()
		b.salt = nil
		b.l.Unlock()
	case path.Join(b.storagePrefix, "policy/metadata"):
		b.l.Lock()
		b.keyPolicy = nil
		b.l.Unlock()
	}
}

// Salt will load a the salt, or if one has not been created yet it will
// generate and store a new salt.
func (b *versionedKVBackend) Salt(ctx context.Context, s logical.Storage) (*salt.Salt, error) {
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
	salt, err := salt.NewSalt(ctx, s, &salt.Config{
		HashFunc: salt.SHA256Hash,
		Location: path.Join(b.storagePrefix, salt.DefaultLocation),
	})
	if err != nil {
		return nil, err
	}
	b.salt = salt
	return salt, nil
}

// Policy loads the key policy for this backend, if one has not been created yet
// it will generate and store a new policy.
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

	// Try loading policy
	policy, err := keysutil.LoadPolicy(ctx, s, path.Join(b.storagePrefix, "policy/metadata"))
	if err != nil {
		return nil, err
	}
	if policy != nil {
		// Store and return the policy
		b.keyPolicy = policy
		return b.keyPolicy, nil
	}

	// Policy didn't exist, create it.
	policy = keysutil.NewPolicy(keysutil.PolicyConfig{
		Name:                 "metadata",
		Type:                 keysutil.KeyType_AES256_GCM96,
		Derived:              true,
		KDF:                  keysutil.Kdf_hkdf_sha256,
		ConvergentEncryption: true,
		StoragePrefix:        b.storagePrefix,
		VersionTemplate:      "{{version}}:",
	})

	err = policy.Rotate(ctx, s)
	if err != nil {
		return nil, err
	}

	b.keyPolicy = policy
	return b.keyPolicy, nil
}

// config takes a storage object and returns a configuration object
func (b *versionedKVBackend) config(ctx context.Context, s logical.Storage) (*Configuration, error) {
	raw, err := s.Get(ctx, path.Join(b.storagePrefix, configPath))
	if err != nil {
		return nil, err
	}

	conf := &Configuration{}
	if raw == nil {
		return conf, nil
	}

	if err := proto.Unmarshal(raw.Value, conf); err != nil {
		return nil, err
	}

	return conf, nil
}

// getVersionKey uses the salt to generate the version key for a specific
// version of a key.
func (b *versionedKVBackend) getVersionKey(ctx context.Context, key string, version uint64, s logical.Storage) (string, error) {
	salt, err := b.Salt(ctx, s)
	if err != nil {
		return "", err
	}

	salted := salt.SaltID(fmt.Sprintf("%s|%d", key, version))

	return path.Join(b.storagePrefix, versionPrefix, salted[0:3], salted[3:]), nil
}

// getKeyMetadata returns the metadata object for the provided key, if no object
// exits it will return nil.
func (b *versionedKVBackend) getKeyMetadata(ctx context.Context, s logical.Storage, key string) (*KeyMetadata, error) {
	policy, err := b.Policy(ctx, s)
	if err != nil {
		return nil, err
	}

	es, err := keysutil.NewEncryptedKeyStorage(keysutil.EncryptedKeyStorageConfig{
		Storage: s,
		Policy:  policy,
		Prefix:  path.Join(b.storagePrefix, metadataPrefix),
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

// writeKeyMetadata writes a metadata object to storage.
func (b *versionedKVBackend) writeKeyMetadata(ctx context.Context, s logical.Storage, meta *KeyMetadata) error {
	policy, err := b.Policy(ctx, s)
	if err != nil {
		return err
	}

	es, err := keysutil.NewEncryptedKeyStorage(keysutil.EncryptedKeyStorageConfig{
		Storage: s,
		Policy:  policy,
		Prefix:  path.Join(b.storagePrefix, metadataPrefix),
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

func ptypesTimestampToString(t *timestamp.Timestamp) string {
	if t == nil {
		return ""
	}

	return ptypes.TimestampString(t)
}

var backendHelp string = `
`
