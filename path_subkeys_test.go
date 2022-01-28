package kv

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Subkeys_NotFound(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || resp != nil {
		t.Fatalf("unexpected ReadOperation response, err: %s, resp %#v", err.Error(), resp)
	}
}

func TestVersionedKV_Subkeys_CurrentVersion(t *testing.T) {
	b, storage := getBackend(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"foo": "does-not-matter",
			"bar": map[string]interface{}{
				"a": map[string]interface{}{
					"c": map[string]interface{}{
						"d": "does-not-matter",
					},
				},
				"b": map[string]interface{}{},
			},
			"baz": map[string]interface{}{
				"e": 3.14,
			},
			"quux": 123,
			"quuz": []string{"does-not-matter"},
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
		t.Fatalf("CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	expectedRespKeys := map[string]struct{}{
		"subkeys":  {},
		"metadata": {},
	}

	if diff := deep.Equal(getKeySet(resp.Data), expectedRespKeys); len(diff) > 0 {
		t.Fatalf("metadata map keys mismatch, diff: %#v", diff)
	}

	metadata, ok := resp.Data["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected metadata to be map, actual: %#v", metadata)
	}

	if diff := deep.Equal(getKeySet(metadata), expectedMetadataKeys()); len(diff) > 0 {
		t.Fatalf("metadata map keys mismatch, diff: %#v", diff)
	}

	expectedSubkeys := map[string]interface{}{
		"foo": nil,
		"bar": map[string]interface{}{
			"a": map[string]interface{}{
				"c": map[string]interface{}{
					"d": nil,
				},
			},
			"b": nil,
		},
		"baz": map[string]interface{}{
			"e": nil,
		},
		"quux": nil,
		"quuz": nil,
	}

	if diff := deep.Equal(resp.Data["subkeys"], expectedSubkeys); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}
}

func TestVersionedKV_Subkeys_EmptyData(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{},
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	if diff := deep.Equal(resp.Data["subkeys"], map[string]interface{}{}); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}
}

func TestVersionedKV_Subkeys_MaxDepth(t *testing.T) {
	b, storage := getBackend(t)

	maxSubkeysDepth = 3

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": map[string]interface{}{
						"baz": 123,
					},
				},
			},
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	expectedSubKeys := map[string]interface{}{
		"foo": map[string]interface{}{
			"bar": map[string]interface{}{
				"baz": nil,
			},
		},
	}

	if diff := deep.Equal(resp.Data["subkeys"], expectedSubKeys); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}
}

func TestVersionedKV_Subkeys_VersionDeleted(t *testing.T) {
	// TODO
}

func TestVersionedKV_Subkeys_VersionDestroyed(t *testing.T) {
	// TODO
}
