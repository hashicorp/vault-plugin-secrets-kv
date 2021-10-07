package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-test/deep"
	"reflect"
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
	time.Sleep(time.Second)

	return b, config.StorageView
}

func TestVersionedKV_Data_Put(t *testing.T) {
	b, storage := getBackend(t)

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
}

func TestVersionedKV_Data_Put_ZeroCas(t *testing.T) {
	b, storage := getBackend(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
		"options": map[string]interface{}{
			"cas": float64(0),
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
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err == nil || (resp != nil && !resp.IsError()) {
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	expectedSubStr := "check-and-set parameter did not match"

	if errorMsg, ok := resp.Data["error"]; !(ok && strings.Contains(errorMsg.(string), expectedSubStr)) {
		t.Fatalf("expected check-and-set validation error, resp: %#v\n", resp)
	}
}

func TestVersionedKV_Data_Get(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp != nil {
		t.Fatalf("Bad response: %#v", resp)
	}

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
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

	if resp.Data["version"] != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if !reflect.DeepEqual(resp.Data["data"], data["data"]) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["metadata"].(map[string]interface{})["version"].(uint64) != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}

	parsed, err := time.Parse(time.RFC3339Nano, resp.Data["metadata"].(map[string]interface{})["created_time"].(string))
	if err != nil {
		t.Fatal(err)
	}

	if !parsed.After(time.Now().Add(-1*time.Minute)) || !parsed.Before(time.Now()) {
		t.Fatalf("Bad response: %#v", resp)
	}
}

func TestVersionedKV_Data_Delete(t *testing.T) {
	b, storage := getBackend(t)

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

	req = &logical.Request{
		Operation: logical.DeleteOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	var httpResp logical.HTTPResponse
	err = json.Unmarshal([]byte(resp.Data["http_raw_body"].(string)), &httpResp)
	if err != nil {
		t.Fatal(err)
	}

	if uint64(httpResp.Data["metadata"].(map[string]interface{})["version"].(float64)) != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}

	parsed, err := time.Parse(time.RFC3339Nano, httpResp.Data["metadata"].(map[string]interface{})["deletion_time"].(string))
	if err != nil {
		t.Fatal(err)
	}

	if !parsed.After(time.Now().Add(-1*time.Minute)) || !parsed.Before(time.Now()) {
		t.Fatalf("Bad response: %#v", resp)
	}

}

func TestVersionedKV_Data_Put_CleanupOldVersions(t *testing.T) {
	b, storage := getBackend(t)

	// Write 10 versions
	for i := 0; i < 10; i++ {
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
			t.Fatalf("data CreateOperation request failed - err:%s resp:%#v\n", err, resp)
		}

		expectedVersion := uint64(i + 1)
		if actualVersion := resp.Data["version"]; actualVersion != expectedVersion {
			t.Fatalf("expected version %d but received %d, resp: %#v", actualVersion, expectedVersion, resp)
		}
	}

	// lower max versions
	data := map[string]interface{}{
		"max_versions": 2,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("metadata CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// write another version
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
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
		t.Fatalf("data CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	expectedVersion := uint64(11)
	if actualVersion := resp.Data["version"]; actualVersion != expectedVersion {
		t.Fatalf("expected version %d but received %d, resp: %#v", actualVersion, expectedVersion, resp)
	}

	// Make sure versions 1-9 were cleaned up.
	for i := 1; i <= 9; i++ {
		versionKey, err := b.(*versionedKVBackend).getVersionKey(context.Background(), "foo", uint64(i), storage)
		if err != nil {
			t.Fatalf("error getting version key for version %d, err: %#v\n", i, err)
		}

		v, err := storage.Get(context.Background(), versionKey)
		if err != nil {
			t.Fatalf("error getting entry for key %s, err: %#v\n", versionKey, err)
		}

		if v != nil {
			t.Fatalf("version not cleaned up %d", i)
		}
	}
}

func TestVersionedKV_Data_Patch_CleanupOldVersions(t *testing.T) {
	b, storage := getBackend(t)

	// Write 10 versions
	for i := 0; i < 10; i++ {
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
			t.Fatalf("data CreateOperation request failed - err:%s resp:%#v\n", err, resp)
		}

		expectedVersion := uint64(i + 1)
		if actualVersion := resp.Data["version"]; actualVersion != expectedVersion {
			t.Fatalf("expected version %d but received %d, resp: %#v", actualVersion, expectedVersion, resp)
		}
	}

	// lower max versions
	data := map[string]interface{}{
		"max_versions": 2,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("metadata CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// write another version
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("data PatchOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	expectedVersion := uint64(11)
	if actualVersion := resp.Data["version"]; actualVersion != expectedVersion {
		t.Fatalf("expected version %d but received %d, resp: %#v", actualVersion, expectedVersion, resp)
	}

	// Make sure versions 1-9 were cleaned up.
	for i := 1; i <= 9; i++ {
		versionKey, err := b.(*versionedKVBackend).getVersionKey(context.Background(), "foo", uint64(i), storage)
		if err != nil {
			t.Fatalf("error getting version key for version %d, err: %#v\n", i, err)
		}

		v, err := storage.Get(context.Background(), versionKey)
		if err != nil {
			t.Fatalf("error getting entry for key %s, err: %#v\n", versionKey, err)
		}

		if v != nil {
			t.Fatalf("version not cleaned up %d", i)
		}
	}
}

func TestVersionedKV_Reload_Policy(t *testing.T) {
	b, storage := getBackend(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
	}

	// Write 10 versions
	for i := 0; i < 10; i++ {

		req := &logical.Request{
			Operation: logical.CreateOperation,
			Path:      fmt.Sprintf("data/%d", i),
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
	}

	b, err := VersionedKVFactory(context.Background(), config)
	if err != nil {
		t.Fatalf("unable to create backend: %v", err)
	}

	// Read values back out
	for i := 0; i < 10; i++ {
		req := &logical.Request{
			Operation: logical.ReadOperation,
			Path:      fmt.Sprintf("data/%d", i),
			Storage:   storage,
		}

		resp, err := b.HandleRequest(context.Background(), req)
		if err != nil || (resp != nil && resp.IsError()) {
			t.Fatalf("err:%s resp:%#v\n", err, resp)
		}

		if !reflect.DeepEqual(resp.Data["data"], data["data"]) {
			t.Fatalf("Bad response: %#v", resp)
		}

	}

}

func TestVersionedKV_Patch_NotFound(t *testing.T) {
	b, storage := getBackend(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
	}

	req := &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || resp != nil {
		t.Fatalf("expected nil response for PatchOperation - err:%s resp:%#v\n", err, resp)
	}

	metadata := map[string]interface{}{
		"max_versions": 5,
	}

	// A patch request should not be allowed if a metadata entry
	// exists but a data entry does not
	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      metadata,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp != nil {
		t.Fatalf("metadata CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp != nil {
		t.Fatalf("expected nil response for PatchOperation - err:%s resp:%#v\n", err, resp)
	}
}

func TestVersionedKV_Patch_CASValidation(t *testing.T) {
	b, storage := getBackend(t)

	config := map[string]interface{}{
		"cas_required": true,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "config",
		Storage:   storage,
		Data:      config,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("CreateOperation request for config failed - err:%s resp:%#v\n", err, resp)
	}

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
		},
		"options": map[string]interface{}{
			"cas": 0,
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
		t.Fatalf("CreateOperation request for data failed - err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(1) {
		t.Fatalf("Version 1 was not created - err:%s resp:%#v\n", err, resp)
	}

	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	// Resp should be error since cas value was not provided but is required
	if err == nil || (resp != nil && !resp.IsError()) {
		t.Fatalf("expected PatchOperation to fail - err:%s resp:%#v\n", err, resp)
	}

	expectedSubStr := "check-and-set parameter required for this call"

	if errorMsg, ok := resp.Data["error"]; !(ok && strings.Contains(errorMsg.(string), expectedSubStr)) {
		t.Fatalf("expected check-and-set validation error, resp: %#v\n", resp)
	}

	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
		"options": map[string]interface{}{
			"cas": float64(2),
		},
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	// Resp should be error since cas value does not match current version
	if err == nil || (resp != nil && !resp.IsError()) {
		t.Fatalf("expected PatchOperation to fail - err:%s resp:%#v\n", err, resp)
	}

	expectedSubStr = "check-and-set parameter did not match"

	if errorMsg, ok := resp.Data["error"]; !(ok && strings.Contains(errorMsg.(string), expectedSubStr)) {
		t.Fatalf("expected check-and-set validation error, resp: %#v\n", resp)
	}
}

func TestVersionedKV_Patch_NoData(t *testing.T) {
	b, storage := getBackend(t)
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
	if err != nil {
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if resp != nil || err == nil || err.Error() != "no data provided" {
		t.Fatalf("expected no data error for PatchOperation - err:%s resp:%#v\n", err, resp)
	}
}

func TestVersionedKV_Patch_Success(t *testing.T) {
	b, storage := getBackend(t)

	data := map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz",
			"quux": map[string]interface{}{
				"quuz": []string{"1", "2", "3"},
			},
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
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}

	data = map[string]interface{}{
		"data": map[string]interface{}{
			"abc": float64(123),
			"quux": map[string]interface{}{
				"def":  float64(456),
				"quuz": []string{"1", "2", "3", "4"},
			},
		},
		"options": map[string]interface{}{
			"cas": float64(1),
		},
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("PatchOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Readperation request failed - err:%s resp:%#v\n", err, resp)
	}

	expectedData := map[string]interface{}{
		"bar": "baz",
		"abc": float64(123),
		"quux": map[string]interface{}{
			"def":  float64(456),
			"quuz": []interface{}{"1", "2", "3", "4"},
		},
	}

	if diff := deep.Equal(resp.Data["data"], expectedData); len(diff) > 0 {
		t.Fatalf("secret data mismatch, diff: %#v\n", diff)
	}
}

func TestVersionedKV_Patch_CurrentVersionDeleted(t *testing.T) {
	b, storage := getBackend(t)

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
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.DeleteOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("DeleteOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody := map[string]interface{}{}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok := respBody["data"]
	if !ok {
		t.Fatalf("No data provided in response, resp: %#v\n", resp)
	}

	respData := respDataRaw.(map[string]interface{})

	respMetadataRaw, ok := respData["metadata"]
	if !ok {
		t.Fatalf("No metadata provided in response, resp: %#v\n", resp)
	}

	respMetadata := respMetadataRaw.(map[string]interface{})

	if respMetadata["deletion_time"] == "" {
		t.Fatalf("Expected deletion_time to be set, resp:%#v\n", resp)
	}

	data["quux"] = "quuz"

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("PatchOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody = map[string]interface{}{}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok = respBody["data"]
	if !ok {
		t.Fatalf("No data provided in response, resp: %#v\n", resp)
	}

	respData = respDataRaw.(map[string]interface{})

	// Unlike the ReadOperation handler, the PatchOperation handler
	// does not ever return secret data. Thus, the secret metadata is
	// returned as top-level keys in the response.
	if resp.Data["http_status_code"] != 404 ||
		respData["version"] != float64(1) ||
		respData["deletion_time"] == "" {
		t.Fatalf("Expected 404 status code for deleted version: resp:%#v\n", resp)
	}
}

func TestVersionedKV_Patch_CurrentVersionDestroyed(t *testing.T) {
	b, storage := getBackend(t)

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
		t.Fatalf("CreateOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	versionsToDestroy := map[string]interface{}{
		"versions": []int{1},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "destroy/foo",
		Storage:   storage,
		Data:      versionsToDestroy,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("DeleteOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("ReadOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody := map[string]interface{}{}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok := respBody["data"]
	if !ok {
		t.Fatalf("No data provided in response, resp: %#v\n", resp)
	}

	respData := respDataRaw.(map[string]interface{})

	respMetadataRaw, ok := respData["metadata"]
	if !ok {
		t.Fatalf("No metadata provided in response, resp: %#v\n", resp)
	}

	respMetadata := respMetadataRaw.(map[string]interface{})

	if respMetadata["destroyed"] == nil || !respMetadata["destroyed"].(bool) {
		t.Fatalf("Expected version to be destroyed, resp:%#v\n", resp)
	}

	data["quux"] = "quuz"

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("PatchOperation request failed - err:%s resp:%#v\n", err, resp)
	}

	// Use of logical.RespondWithStatusCode in handler will
	// serialize the JSON response body as a string
	respBody = map[string]interface{}{}

	if rawRespBody, ok := resp.Data[logical.HTTPRawBody]; ok {
		err = json.Unmarshal([]byte(rawRespBody.(string)), &respBody)
	}

	respDataRaw, ok = respBody["data"]
	if !ok {
		t.Fatalf("No data provided in response, resp: %#v\n", resp)
	}

	respData = respDataRaw.(map[string]interface{})

	respMetadata = respMetadataRaw.(map[string]interface{})

	// Unlike the ReadOperation handler, the PatchOperation handler
	// does not ever return secret data. Thus, the secret metadata is
	// returned as top-level keys in the response.
	if resp.Data["http_status_code"] != 404 ||
		respData["version"] != float64(1) ||
		(respData["destroyed"] == nil || !respData["destroyed"].(bool)) {
		t.Fatalf("Expected 404 status code for destroyed version: resp:%#v\n", resp)
	}
}
