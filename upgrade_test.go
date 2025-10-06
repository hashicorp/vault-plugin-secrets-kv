// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/helper/logging"
	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Upgrade(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b, storage := testPassthroughBackendWithStorage()

		keyCount := 10
		for i := 0; i < keyCount; i++ {
			data := map[string]interface{}{
				"bar": i,
			}

			req := &logical.Request{
				Operation: logical.CreateOperation,
				Path:      fmt.Sprintf("%d/foo", i),
				Storage:   storage,
				Data:      data,
			}

			resp, err := b.HandleRequest(context.Background(), req)
			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("err:%s resp:%#v\n", err, resp)
			}
		}

		config := &logical.BackendConfig{
			Logger:      logging.NewVaultLogger(log.Trace),
			System:      &logical.StaticSystemView{},
			StorageView: storage,
			BackendUUID: "test",
			Config: map[string]string{
				"version": "2",
				"upgrade": "true",
			},
		}

		var err error
		b, err = Factory(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}

		b.(*versionedKVBackend).blockUpgrades = make(chan struct{}, 1)

		// Because we have v1 stored keys already, Initialize
		// will launch a goroutine to upgrade them.
		err = b.Initialize(t.Context(), &logical.InitializationRequest{
			Storage: storage,
		})
		if err != nil {
			t.Fatalf("Initialize failure: %v", err)
		}

		// verify requests are rejected during upgrade
		req := &logical.Request{
			Operation: logical.ReadOperation,
			Path:      fmt.Sprintf("data/%d/foo", 1),
			Storage:   storage,
		}

		resp, err := b.HandleRequest(context.Background(), req)
		if resp == nil || resp.Error().Error() != "Upgrading from non-versioned to versioned data. This backend will be unavailable for a brief period and will resume service shortly." {
			t.Fatalf("err:%s resp:%#v\n", err, resp)
		}

		// wait for upgrade to finish
		b.(*versionedKVBackend).blockUpgrades <- struct{}{}
		synctest.Wait()

		for i := 0; i < keyCount; i++ {
			data := map[string]interface{}{
				"bar": float64(i),
			}

			req := &logical.Request{
				Operation: logical.ReadOperation,
				Path:      fmt.Sprintf("data/%d/foo", i),
				Storage:   storage,
			}

			resp, err := b.HandleRequest(context.Background(), req)
			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("err:%s resp:%#v\n", err, resp)
			}

			if !reflect.DeepEqual(resp.Data["data"].(map[string]interface{}), data) {
				t.Fatalf("bad response %#v", resp)
			}
		}
	})
}
