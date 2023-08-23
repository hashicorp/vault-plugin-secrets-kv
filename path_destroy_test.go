// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

import (
	"context"
	"testing"

	"github.com/hashicorp/vault/sdk/helper/testhelpers/schema"
	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Destroy_Put(t *testing.T) {
	b, storage, events := getBackendWithEvents(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}

	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
		"options": map[string]interface{}{
			"cas": float64(1),
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

	data = map[string]interface{}{
		"versions": "1,2",
	}

	req = &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "destroy/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["versions"].(map[string]interface{})["1"].(map[string]interface{})["destroyed"].(bool) != true {
		t.Fatalf("Bad response: %#v", resp)
	}
	if resp.Data["versions"].(map[string]interface{})["2"].(map[string]interface{})["destroyed"].(bool) != true {
		t.Fatalf("Bad response: %#v", resp)
	}

	events.expectEvents(t, []expectedEvent{
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/destroy", "destroy/foo", ""},
	})
}
