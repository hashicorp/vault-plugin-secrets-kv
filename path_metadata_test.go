package kv

import (
	"context"
	"fmt"
	"github.com/go-test/deep"
	"testing"
	"time"

	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Metadata_Put(t *testing.T) {
	b, storage := getBackend(t)

	d := 5 * time.Minute
	data := map[string]interface{}{
		"max_versions":         2,
		"cas_required":         true,
		"delete_version_after": d.String(),
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["max_versions"] != uint32(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["cas_required"] != true {
		t.Fatalf("Bad response: %#v", resp)
	}
	if resp.Data["delete_version_after"] != d.String() {
		t.Fatalf("Bad response: %#v", resp)
	}

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
	if err == nil || resp.Error().Error() != "check-and-set parameter required for this call" {
		t.Fatalf("expected error, %#v", resp)
	}

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
	if err != nil || resp == nil || resp.IsError() {
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
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

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
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(3) {
		t.Fatalf("Bad response: %#v", resp)
	}

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
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

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
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["version"] != uint64(4) {
		t.Fatalf("Bad response: %#v", resp)
	}

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

func TestVersionedKV_Metadata_List_Recursive(t *testing.T) {
	b, storage := getBackend(t)

	testData := []struct {
		Path string
		Data map[string]interface{}
	}{
		{
			Path: "level1",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level2/childlevel1",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level2/childlevel2",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level3/childlevel1/childchildlevel1",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level3/childlevel2/childchildlevel2",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level4/childlevel1/childchildlevel1/childchildchildlevel1",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level4/childlevel2/childchildlevel2/childchildchildlevel2",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
		{
			Path: "level4/childlevel3/childchildlevel1/childchildchildlevel1",
			Data: map[string]interface{}{
				"data": map[string]interface{}{
					"bar": "foo",
				},
			},
		},
	}

	// Insert test data
	for _, data := range testData {
		req := &logical.Request{
			Operation: logical.CreateOperation,
			Path:      "data/" + data.Path,
			Storage:   storage,
			Data:      data.Data,
		}

		resp, err := b.HandleRequest(context.Background(), req)
		if err != nil || (resp != nil && resp.IsError()) {
			t.Fatalf("err:%s resp:%#v\n", err, resp)
		}
	}

	// Read all data
	req := &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata-recursive/",
		Storage:   storage,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Define expected response
	expectedResp := map[string]interface{}{
		"keys": []string{
			"level1",
			"level2/",
			"level2/childlevel1",
			"level2/childlevel2",
			"level3/",
			"level3/childlevel1/",
			"level3/childlevel1/childchildlevel1",
			"level3/childlevel2/",
			"level3/childlevel2/childchildlevel2",
			"level4/",
			"level4/childlevel1/",
			"level4/childlevel1/childchildlevel1/",
			"level4/childlevel1/childchildlevel1/childchildchildlevel1",
			"level4/childlevel2/",
			"level4/childlevel2/childchildlevel2/",
			"level4/childlevel2/childchildlevel2/childchildchildlevel2",
			"level4/childlevel3/",
			"level4/childlevel3/childchildlevel1/",
			"level4/childlevel3/childchildlevel1/childchildchildlevel1",
		},
	}

	// Validate response
	if resp.Data == nil {
		t.Fatal("expected response data but was nil")
	}
	if diff := deep.Equal(resp.Data, expectedResp); len(diff) != 0 {
		t.Fatalf("Response is not expected. Diff: %#v", diff)
	}
}
