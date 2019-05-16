package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Metadata_Put(t *testing.T) {
	b, storage := getBackend(t)

	// begin set metadata ... T0
	d := 5 * time.Minute
	data := map[string]interface{}{
		"max_versions": 2,
		"cas_required": true,
		"version_ttl":  d.String(),
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	// want response and error to be nil
	// do not want error or a nil response or a response that is an error
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	// ... end set metadata

	// begin read metadata ... T0.1
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["max_versions"] != uint32(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["cas_required"] != true {
		t.Fatalf("Bad response: %#v", resp)
	}
	if resp.Data["version_ttl"] != d.String() {
		t.Fatalf("Bad response: %#v", resp)
	}

	// ... end read metadata

	// begin set data ... T1
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	// Should fail with with cas required error
	resp, err = b.HandleRequest(context.Background(), req)
	// want error and response, and want response that is an error with a specific
	// message
	if err == nil || resp.Error().Error() != "check-and-set parameter required for this call" {
		t.Fatalf("expected error, %#v", resp)
	}
	// ... end set data

	// begin set data ... T2
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
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
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// check response for version set to 1
	if resp.Data["version"] != uint64(1) {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end set data

	// begin set data ... T3
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
		"options": map[string]interface{}{
			"cas": 1,
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// check response for version set to 2
	if resp.Data["version"] != uint64(2) {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end set data

	// begin set data ... T4
	// ... do a write operation with the same key and value
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
		"options": map[string]interface{}{
			"cas": 2,
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// check response for version set to 3
	// writing the same value should still increment the version
	if resp.Data["version"] != uint64(3) {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end set data

	// begin read metadata ... T5
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["current_version"] != uint64(3) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["oldest_version"] != uint64(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if _, ok := resp.Data["versions"].(map[string]interface{})["2"]; !ok {
		t.Fatalf("Bad response: %#v", resp)
	}

	if _, ok := resp.Data["versions"].(map[string]interface{})["3"]; !ok {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end read metadata

	// begin update metadata ... T6
	// Update the metadata settings, remove the cas requirement and lower the
	// max versions.
	data = map[string]interface{}{
		"max_versions": 1,
		"cas_required": false,
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error or a nil response or a response that is an error
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	// ... end update metadata

	// begin set data ... T7
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"bar": "baz1",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(4) {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end set data

	// begin read metadata ... T8
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	// do not want error, want response and it can't be an error response
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["current_version"] != uint64(4) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["oldest_version"] != uint64(4) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if _, ok := resp.Data["versions"].(map[string]interface{})["4"]; !ok {
		t.Fatalf("Bad response: %#v", resp)
	}

	if len(resp.Data["versions"].(map[string]interface{})) != 1 {
		t.Fatalf("Bad response: %#v", resp)
	}
	// ... end read metadata
}

func TestVersionedKV_Metadata_Delete(t *testing.T) {
	b, storage := getBackend(t)

	// Create a few versions
	for i := 0; i <= 5; i++ {
		data := map[string]interface{}{
			"data": map[string]interface{}{
				"bar": fmt.Sprintf("baz%d", i),
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

		if resp.Data["version"] != uint64(i+1) {
			t.Fatalf("Bad response: %#v", resp)
		}
	}

	req := &logical.Request{
		Operation: logical.DeleteOperation,
		Path:      "metadata/foo",
		Storage:   storage,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Read the data path
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "data/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	if resp != nil {
		t.Fatalf("Bad response: %#v", resp)
	}

	// Read the metadata path
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	if resp != nil {
		t.Fatalf("Bad response: %#v", resp)
	}

	// Verify all the version data was deleted.
	for i := 0; i <= 5; i++ {
		versionKey, err := b.(*versionedKVBackend).getVersionKey(context.Background(), "foo", uint64(i+1), req.Storage)
		if err != nil {
			t.Fatal(err)
		}

		v, err := storage.Get(context.Background(), versionKey)
		if err != nil {
			t.Fatal(err)
		}

		if v != nil {
			t.Fatal("Version wasn't deleted")
		}

	}

}
