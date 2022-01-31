package kv

import (
	"context"
	"encoding/json"
	"net/http"
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

func TestVersionedKV_Subkeys_VersionParam(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{
				"foo": "abc",
			},
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("data CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{
				"foo": "abc",
				"bar": "def",
			},
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("data CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
		Data:      map[string]interface{}{
			"version": 1,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("subkeys ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	expectedSubkeys := map[string]interface{}{
		"foo": nil,
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
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{
				"foo": "bar",
			},
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("data CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("subkeys ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	expectedSubkeys := map[string]interface{}{
		"foo": nil,
	}
	if diff := deep.Equal(resp.Data["subkeys"], expectedSubkeys); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}

	req = &logical.Request{
		Operation: logical.DeleteOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("metadata DeleteOperation request failed - err: %s resp: %#v\n", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("subkeys ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody := map[string]interface{}{}

	if resp.Data["http_status_code"] != http.StatusNotFound {
		t.Fatalf("expected 404 response for subkeys ReadOperation: resp: %#v", resp)
	}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok := respBody["data"]
	if !ok {
		t.Fatalf("no data provided in subkeys response, resp: %#v\n", resp)
	}

	respData := respDataRaw.(map[string]interface{})

	respMetadataRaw, ok := respData["metadata"]
	if !ok {
		t.Fatalf("no metadata provided in subkeys response, resp: %#v\n", resp)
	}

	respMetadata := respMetadataRaw.(map[string]interface{})

	if respMetadata["deletion_time"] == "" {
		t.Fatalf("expected deletion_time to be set, resp: %#v\n", resp)
	}
}

func TestVersionedKV_Subkeys_VersionDestroyed(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"data": map[string]interface{}{
				"foo": "bar",
			},
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("data CreateOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("subkeys ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	expectedSubkeys := map[string]interface{}{
		"foo": nil,
	}
	if diff := deep.Equal(resp.Data["subkeys"], expectedSubkeys); len(diff) > 0 {
		t.Fatalf("resp and expected data mismatch, diff: %#v", diff)
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "destroy/foo",
		Storage:   storage,
		Data:      map[string]interface{}{
			"versions": []int{1},
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("destroy CreateOperation request failed - err: %s resp:%#v\n", err.Error(), resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "subkeys/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("subkeys ReadOperation request failed, err: %s, resp %#v", err.Error(), resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody := map[string]interface{}{}

	if resp.Data["http_status_code"] != http.StatusNotFound {
		t.Fatalf("expected 404 response for subkeys ReadOperation: resp:%#v", resp)
	}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok := respBody["data"]
	if !ok {
		t.Fatalf("no data provided in subkeys response, resp: %#v\n", resp)
	}

	respData := respDataRaw.(map[string]interface{})

	respMetadataRaw, ok := respData["metadata"]
	if !ok {
		t.Fatalf("no metadata provided in subkeys response, resp: %#v\n", resp)
	}

	respMetadata := respMetadataRaw.(map[string]interface{})

	if respMetadata["destroyed"] == nil || !respMetadata["destroyed"].(bool) {
		t.Fatalf("expected version to be destroyed, resp: %#v\n", resp)
	}
}
