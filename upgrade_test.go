package vkv

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/hashicorp/vault/helper/logformat"
	"github.com/hashicorp/vault/logical"
	log "github.com/mgutz/logxi/v1"
)

func TestVersionedKV_Upgrade(t *testing.T) {
	b, storage := testPassthroughBackendWithStorage()

	for i := 0; i < 1024*1024; i++ {
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
		Logger:      logformat.NewVaultLogger(log.LevelTrace),
		System:      &logical.StaticSystemView{},
		StorageView: storage,
		Config: map[string]string{
			"versioned": "true",
			"upgrade":   "true",
		},
	}

	var err error
	b, err = Factory(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1024*1024; i++ {
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
}