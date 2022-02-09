package kv

import (
	"context"
	"strings"
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
	timeout := time.After(20 * time.Second)
	ticker := time.Tick(time.Second)

	for {
		select {
		case <-timeout:
			t.Fatal("timeout expired waiting for upgrade")
		case <-ticker:
			req := &logical.Request{
				Operation: logical.ReadOperation,
				Path:      "config",
				Storage:   config.StorageView,
			}

			resp, err := b.HandleRequest(context.Background(), req)
			if err != nil {
				t.Fatalf("unable to read config: %s", err.Error())
				return nil, nil
			}

			if resp != nil && !resp.IsError() {
				return b, config.StorageView
			}

			if resp == nil || (resp.IsError() && strings.Contains(resp.Error().Error(), "Upgrading from non-versioned to versioned")) {
				t.Log("waiting for upgrade to complete")
			}
		}
	}
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
