package kv

import (
	"context"
	"testing"
	"time"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/helper/logging"
	"github.com/hashicorp/vault/sdk/logical"
)

func getBackend(t *testing.T) (logical.Backend, logical.Storage) {
	config := &logical.BackendConfig{
		Logger:      logging.NewVaultLogger(log.Trace),
		System:      &logical.StaticSystemView{},
		StorageView: &logical.InmemStorage{},
		BackendUUID: "test",
	}

	b, err := VersionedKVFactory(context.Background(), config)
	if err != nil {
		t.Fatalf("unable to create backend: %v", err)
	}

	// Wait for the upgrade to finish
	time.Sleep(time.Second)

	return b, config.StorageView
}

// getKeySet will produce a set of the keys that exist in m
func getKeySet(m map[string]interface{}) map[string]struct{} {
	set := make(map[string]struct{})

	for k := range m {
		set[k] = struct{}{}
	}

	return set
}

// expectedMetadataKeys produces a deterministic set of expected
// metadata keys to ensure consistent shape across all endpoints
func expectedMetadataKeys() map[string]struct{} {
	return map[string]struct{}{
		"version":         {},
		"created_time":    {},
		"deletion_time":   {},
		"destroyed":       {},
		"custom_metadata": {},
	}
}
