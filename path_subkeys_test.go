package kv

import (
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Subkeys_Read_CurrentVersion(t *testing.T) {
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
				"b": "does-not-matter",
			},
			"baz": map[string]interface{}{
				"e": "does-not-matter",
			},
			"quux": "does-not-matter",
			"quuz": "does-not-matter",
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
		t.Fatalf("CreateOperation request failed, err: %s, resp %#v", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed, err: %s, resp %#v", err, resp)
	}

	expectedData := map[string]interface{}{
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

	if diff := deep.Equal(resp.Data["subkeys"], expectedData); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}
}
