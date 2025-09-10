// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"
	"github.com/hashicorp/vault/sdk/helper/testhelpers/schema"
	"github.com/hashicorp/vault/sdk/logical"
)

// assertKeysMatch checks that the actual keys slice contains exactly the expected keys
// in any order, providing helpful error messages if they don't match
func assertKeysMatch(t *testing.T, actual []string, expected []string, context string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("%s: expected %d keys, got %d: %v", context, len(expected), len(actual), actual)
	}
	for _, expectedKey := range expected {
		if !slices.Contains(actual, expectedKey) {
			t.Fatalf("%s: missing key %s, should be contained within %v", context, expectedKey, actual)
		}
	}
}

func TestVersionedKV_Metadata_Put(t *testing.T) {
	b, storage := getBackend(t)

	d := 5 * time.Minute

	expectedCustomMetadata := map[string]string{
		"foo": "abc",
		"bar": "123",
	}

	data := map[string]interface{}{
		"max_versions":         2,
		"cas_required":         true,
		"delete_version_after": d.String(),
		"custom_metadata":      expectedCustomMetadata,
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
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if resp.Data["max_versions"] != uint32(2) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["cas_required"] != true {
		t.Fatalf("Bad response: %#v", resp)
	}
	if resp.Data["delete_version_after"] != d.String() {
		t.Fatalf("Bad response: %#v", resp)
	}

	if diff := deep.Equal(resp.Data["custom_metadata"], expectedCustomMetadata); len(diff) > 0 {
		t.Fatal(diff)
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

	type testCase struct {
		name        string
		data        map[string]interface{}
		displayName string
		entityID    string
		clientID    string
		cas         uint64
		expVersion  uint64
	}

	tests := []testCase{
		{
			name:        "version 1",
			data:        map[string]interface{}{"bar": "baz1"},
			displayName: "Tester1",
			entityID:    "11111111-1111-1111-1111-111111111111",
			clientID:    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
			cas:         0,
			expVersion:  1,
		},
		{
			name:        "version 2",
			data:        map[string]interface{}{"bar": "baz2"},
			displayName: "Tester2",
			entityID:    "22222222-2222-2222-2222-222222222222",
			clientID:    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
			cas:         1,
			expVersion:  2,
		},
		{
			name:        "version 3",
			data:        map[string]interface{}{"bar": "baz3"},
			displayName: "Tester3",
			entityID:    "33333333-3333-3333-3333-333333333333",
			clientID:    "cccccccc-cccc-cccc-cccc-cccccccccccc",
			cas:         2,
			expVersion:  3,
		},
	}

	for _, tc := range tests {
		data := map[string]interface{}{
			"data":    tc.data,
			"options": map[string]interface{}{"cas": tc.cas},
		}
		req := &logical.Request{
			Operation:   logical.CreateOperation,
			Path:        "data/foo",
			Storage:     storage,
			Data:        data,
			DisplayName: tc.displayName,
			EntityID:    tc.entityID,
			ClientID:    tc.clientID,
		}
		resp, err = b.HandleRequest(context.Background(), req)
		if err != nil || resp == nil || resp.IsError() {
			t.Fatalf("[%s] err:%s resp:%#v\n", tc.name, err, resp)
		}

		if resp.Data["version"] != tc.expVersion {
			t.Fatalf("[%s] Bad response: %#v", tc.name, resp)
		}

		// Metadata read test
		req = &logical.Request{
			Operation: logical.ReadOperation,
			Path:      "metadata/foo",
			Storage:   storage,
		}
		resp, err = b.HandleRequest(context.Background(), req)
		if err != nil || resp == nil || resp.IsError() {
			t.Fatalf("err:%s resp:%#v\n", err, resp)
		}
		schema.ValidateResponse(
			t,
			schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
			resp,
			true,
		)

		versions := resp.Data["versions"].(map[string]interface{})
		spew.Dump(versions)
		latestVersion := strconv.Itoa(len(versions))
		fmt.Printf("Latest version: %s\n", latestVersion)
		latest := versions[latestVersion].(map[string]interface{})
		actor := latest["created_by"].(*Attribution).Actor
		entity := latest["created_by"].(*Attribution).EntityId
		client := latest["created_by"].(*Attribution).ClientId

		if actor != tc.displayName {
			spew.Dump("ERROR", versions)
			t.Fatalf("mistmatching attribution Actor for version %s: expected %s, got %s", latestVersion, tc.displayName, actor)
		}
		if entity != tc.entityID {
			spew.Dump("ERROR", versions)
			t.Fatalf("mistmatching attribution EntityID for version %s: expected %s, got %s", latestVersion, tc.entityID, entity)
		}
		if client != tc.clientID {
			spew.Dump("ERROR", versions)
			t.Fatalf("mistmatching attribution ClientID for version %s: expected %s, got %s", latestVersion, tc.clientID, client)
		}

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
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

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
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

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
	b, storage, events := getBackendWithEvents(t)

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
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

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

	events.expectEvents(t, []expectedEvent{
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/data-write", "data/foo", "data/foo"},
		{"kv-v2/metadata-delete", "metadata/foo", ""},
	})
}

func TestVersionedKV_Metadata_Put_Bad_CustomMetadata(t *testing.T) {
	b, storage := getBackend(t)

	metadataPath := "metadata/foo"

	stringToRepeat := "a"
	longKeyLength := 129
	longKey := strings.Repeat(stringToRepeat, longKeyLength)

	longValueKey := "long_value"
	longValueLength := 513

	emptyValueKey := "empty_value"
	unprintableString := "unprint\u200bable"
	unprintableValueKey := "unprintable"

	customMetadata := map[string]interface{}{
		longValueKey:        strings.Repeat(stringToRepeat, longValueLength),
		longKey:             "abc123",
		"":                  "abc123",
		emptyValueKey:       "",
		unprintableString:   "abc123",
		unprintableValueKey: unprintableString,
	}

	data := map[string]interface{}{
		"custom_metadata": customMetadata,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil {
		t.Fatalf("Write err: %s resp: %#v\n", err, resp)
	}

	// Should fail with validation errors
	if !resp.IsError() {
		t.Fatalf("expected resp error, resp: %#v", resp)
	}

	respError := resp.Error().Error()

	if keyCount := len(customMetadata); !strings.Contains(respError, fmt.Sprintf("%d errors occurred", keyCount)) {
		t.Fatalf("Expected %d validation errors, resp: %#v", keyCount, resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("length of key %q is %d",
		longKey,
		longKeyLength)) {
		t.Fatalf("Expected key length error for key %q, resp: %#v", longKey, resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("length of value for key %q is %d",
		longValueKey,
		longValueLength)) {
		t.Fatalf("Expected value length error for key %q, resp: %#v", longValueKey, resp)
	}

	if !strings.Contains(respError, "length of key \"\" is 0") {
		t.Fatalf("Expected key length error for key \"\", resp: %#v", resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("length of value for key %q is 0", emptyValueKey)) {
		t.Fatalf("Expected value length error for key %q, resp: %#v", emptyValueKey, resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("key %q (%s) contains unprintable", unprintableString, unprintableString)) {
		t.Fatalf("Expected unprintable character error for key %q, resp: %#v", unprintableString, resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("key %q contains unprintable", unprintableValueKey)) {
		t.Fatalf("Expected unpritnable character for value of key %q, resp: %#v", unprintableValueKey, resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("Read err: %#v, resp: %#v", err, resp)
	}

	if resp != nil {
		t.Fatalf("Expected empty read due to validation errors, resp: %#v", resp)
	}

	data = map[string]interface{}{
		"custom_metadata": map[string]interface{}{
			"foo": map[string]interface{}{
				"bar": "baz",
			},
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil {
		t.Fatalf("Write err: %s resp: %#v\n", err, resp)
	}

	if !resp.IsError() {
		t.Fatalf("expected resp error, resp: %#v", resp)
	}

	respError = resp.Error().Error()
	expectedError := "got unconvertible type"

	if !strings.Contains(respError, expectedError) {
		t.Fatalf("expected response error %q to include %q validation errors", respError, expectedError)
	}
}

func TestVersionedKv_Metadata_Put_Too_Many_CustomMetadata_Keys(t *testing.T) {
	b, storage := getBackend(t)

	metadataPath := "metadata/foo"

	customMetadata := map[string]string{}

	for i := 0; i < maxCustomMetadataKeys+1; i++ {
		k := fmt.Sprint(i)
		customMetadata[k] = k
	}

	data := map[string]interface{}{
		"custom_metadata": customMetadata,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil {
		t.Fatalf("Write err: %s resp: %#v\n", err, resp)
	}

	if !resp.IsError() {
		t.Fatalf("expected resp error, resp: %#v", resp)
	}

	respError := resp.Error().Error()

	if !strings.Contains(respError, "1 error occurred") {
		t.Fatalf("Expected 1 validation error, resp: %#v", resp)
	}

	if !strings.Contains(respError, fmt.Sprintf("payload must contain at most %d keys, provided %d",
		maxCustomMetadataKeys,
		len(customMetadata))) {
		t.Fatalf("Expected max custom metadata keys error, resp: %#v", resp)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("Read err: %#v, resp :%#v", err, resp)
	}

	if resp != nil {
		t.Fatalf("Expected empty read due to validation errors, resp: %#v", resp)
	}
}

func TestVersionedKV_Metadata_Put_Empty_CustomMetadata(t *testing.T) {
	b, storage := getBackend(t)

	metadataPath := "metadata/foo"

	data := map[string]interface{}{
		"custom_metadata": map[string]string{},
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Write err: %s, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("Read err: %s, resp %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	// writing custom_metadata as {} should result in nil
	if diff := deep.Equal(resp.Data["custom_metadata"], map[string]string(nil)); len(diff) > 0 {
		t.Fatal(diff)
	}
}

func TestVersionedKV_Metadata_Put_Merge_Behavior(t *testing.T) {
	b, storage := getBackend(t)

	metadataPath := "metadata/foo"
	expectedMaxVersions := uint32(5)
	expectedCasRequired := true

	data := map[string]interface{}{
		"max_versions": expectedMaxVersions,
		"cas_required": expectedCasRequired,
	}

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Write err: %s, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("Read err: %s, resp %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if resp.Data["max_versions"] != expectedMaxVersions {
		t.Fatalf("max_versions mismatch, expected: %d, actual: %d, resp: %#v",
			expectedMaxVersions,
			resp.Data["max_versions"],
			resp)
	}

	if resp.Data["cas_required"] != expectedCasRequired {
		t.Fatalf("cas_required mismatch, expected: %t, actual: %t, resp: %#v",
			expectedCasRequired,
			resp.Data["cas_required"],
			resp)
	}

	// custom_metadata was not provided so it should come back as a nil map
	if diff := deep.Equal(resp.Data["custom_metadata"], map[string]string(nil)); len(diff) > 0 {
		t.Fatal(diff)
	}

	expectedCasRequired = false
	expectedCustomMetadata := map[string]string{
		"foo": "abc",
		"bar": "123",
	}

	data = map[string]interface{}{
		"cas_required":    expectedCasRequired,
		"custom_metadata": expectedCustomMetadata,
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Write err: %s, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("Read err: %s, resp %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	// max_versions not provided, should not have changed
	if resp.Data["max_versions"] != expectedMaxVersions {
		t.Fatalf("max_versions mismatch, expected: %d, actual: %d, resp: %#v",
			expectedMaxVersions,
			resp.Data["max_versions"],
			resp)
	}

	// cas_required should be overwritten
	if resp.Data["cas_required"] != expectedCasRequired {
		t.Fatalf("cas_required mismatch, expected: %t, actual: %t, resp: %#v",
			expectedCasRequired,
			resp.Data["cas_required"],
			resp)
	}

	// custom_metadata provided for the first time, should no longer be a nil map
	if diff := deep.Equal(resp.Data["custom_metadata"], expectedCustomMetadata); len(diff) > 0 {
		t.Fatal(diff)
	}

	expectedCustomMetadata = map[string]string{
		"baz": "abc123",
	}

	data = map[string]interface{}{
		"custom_metadata": expectedCustomMetadata,
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Write err: %s, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("Read err: %s, resp %#v", err, resp)
	}

	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)
	// max_versions not provided, should not have changed
	if resp.Data["max_versions"] != expectedMaxVersions {
		t.Fatalf("max_versions mismatch, expected: %d, actual: %d",
			expectedMaxVersions,
			resp.Data["max_versions"])
	}

	// cas_required not provided, should not have changed
	if resp.Data["cas_required"] != expectedCasRequired {
		t.Fatalf("cas_required mismatch, expected: %t, actual: %t,",
			expectedCasRequired,
			resp.Data["cas_required"])
	}

	// custom_metadata should be completely overwritten
	if diff := deep.Equal(resp.Data["custom_metadata"], expectedCustomMetadata); len(diff) > 0 {
		t.Fatal(diff)
	}

	expectedMaxVersions = 20

	data = map[string]interface{}{
		"max_versions": expectedMaxVersions,
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      metadataPath,
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("Write err: %s, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      metadataPath,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("Read err: %s, resp %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	// custom_metadata not provided, should not have changed
	if diff := deep.Equal(resp.Data["custom_metadata"], expectedCustomMetadata); len(diff) > 0 {
		t.Fatal(diff)
	}
}

func TestVersionedKV_Metadata_Patch_MissingPath(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"cas_required": true,
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil {
		t.Fatalf("unexpected patch error, err: %#v, resp: %#v", err, resp)
	}

	expectedErr := "missing path"
	if respErr := resp.Error().Error(); !strings.Contains(respErr, expectedErr) {
		t.Fatalf("expected patch output to contain %s, actual: %s", expectedErr, respErr)
	}
}

func TestVersionedKV_Metadata_Patch_Validation(t *testing.T) {
	t.Parallel()

	unprintableString := "unprint\u200bable"

	longKeyLength := 129
	longValueLength := 513

	longKey := strings.Repeat("a", longKeyLength)
	longValue := strings.Repeat("a", longValueLength)

	cases := []struct {
		name     string
		metadata map[string]interface{}
		output   string
	}{
		{
			"field_conversion_error",
			map[string]interface{}{
				"max_versions": []int{1, 2, 3},
			},
			"Field validation failed: error converting input",
		},
		{
			"custom_metadata_empty_key",
			map[string]interface{}{
				"custom_metadata": map[string]string{
					"": "foo",
				},
			},
			fmt.Sprintf("length of key %q is 0", ""),
		},
		{
			"custom_metadata_unprintable_key",
			map[string]interface{}{
				"custom_metadata": map[string]string{
					unprintableString: "foo",
				},
			},
			fmt.Sprintf("key %q (%s) contains unprintable characters", unprintableString, unprintableString),
		},
		{
			"custom_metadata_unprintable_value",
			map[string]interface{}{
				"custom_metadata": map[string]string{
					"foo": unprintableString,
				},
			},
			fmt.Sprintf("value for key %q contains unprintable characters", "foo"),
		},
		{
			"custom_metadata_key_too_long",
			map[string]interface{}{
				"custom_metadata": map[string]string{
					longKey: "foo",
				},
			},
			fmt.Sprintf("length of key %q is %d", longKey, longKeyLength),
		},
		{
			"custom_metadata_value_too_long",
			map[string]interface{}{
				"custom_metadata": map[string]string{
					"foo": longValue,
				},
			},
			fmt.Sprintf("length of value for key %q is %d", "foo", longValueLength),
		},
		{
			"custom_metadata_invalid_type",
			map[string]interface{}{
				"custom_metadata": map[string]interface{}{
					"foo": map[string]interface{}{
						"bar": "baz",
					},
				},
			},
			"got unconvertible type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, storage := getBackend(t)
			path := "metadata/" + tc.name

			req := &logical.Request{
				Operation: logical.CreateOperation,
				Path:      path,
				Storage:   storage,
				Data: map[string]interface{}{
					"cas_required": true,
				},
			}

			resp, err := b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("create request failed, err: %#v, resp: %#v", err, resp)
			}

			req = &logical.Request{
				Operation: logical.PatchOperation,
				Path:      path,
				Storage:   storage,
				Data:      tc.metadata,
			}

			resp, err = b.HandleRequest(context.Background(), req)
			if err != nil {
				t.Fatalf("unexpected patch error, err: %#v", err)
			}

			if resp == nil || !resp.IsError() {
				t.Fatalf("expected patch response to be error, actual: %#v", resp)
			}

			respError := resp.Error().Error()

			if !strings.Contains(respError, tc.output) {
				t.Fatalf("expected patch output to contain %s, actual: %s", tc.output, respError)
			}
		})
	}
}

func TestVersionedKV_Metadata_Patch_NotFound(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"cas_required": true,
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("request failed, err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["http_status_code"] != 404 {
		t.Fatalf("expected 404 response, resp:%#v", resp)
	}
}

func TestVersionedKV_Metadata_Patch_CasRequiredWarning(t *testing.T) {
	b, storage := getBackend(t)

	req := &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "config",
		Storage:   storage,
		Data: map[string]interface{}{
			"cas_required": true,
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("config request failed, err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"max_versions": 5,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("metadata create request failed, err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      "metadata/foo",
		Storage:   storage,
		Data: map[string]interface{}{
			"cas_required": false,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("metadata patch request failed, err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if len(resp.Warnings) != 1 ||
		!strings.Contains(resp.Warnings[0], "\"cas_required\" set to false, but is mandated by backend config") {
		t.Fatalf("expected cas_required warning, resp warnings: %#v", resp.Warnings)
	}

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/foo",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("metadata create request failed, err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if resp.Data["cas_required"] != false {
		t.Fatalf("expected cas_required to be set to false despite warning")
	}
}

func TestVersionedKV_Metadata_Patch_CustomMetadata(t *testing.T) {
	t.Parallel()

	initialCustomMetadata := map[string]string{
		"foo": "abc",
		"bar": "def",
	}

	cases := []struct {
		name   string
		input  map[string]interface{}
		output map[string]string
	}{
		{
			"empty_object",
			map[string]interface{}{},
			map[string]string{
				"foo": "abc",
				"bar": "def",
			},
		},
		{
			"add_a_key",
			map[string]interface{}{
				"baz": "ghi",
			},
			map[string]string{
				"foo": "abc",
				"bar": "def",
				"baz": "ghi",
			},
		},
		{
			"remove_a_key",
			map[string]interface{}{
				"foo": nil,
			},
			map[string]string{
				"bar": "def",
			},
		},
		{
			"replace_a_key",
			map[string]interface{}{
				"foo": "ghi",
			},
			map[string]string{
				"foo": "ghi",
				"bar": "def",
			},
		},
		{
			"mixed",
			map[string]interface{}{
				"foo": "def",
				"bar": nil,
				"baz": "ghi",
			},
			map[string]string{
				"foo": "def",
				"baz": "ghi",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, storage := getBackend(t)

			path := "metadata/" + tc.name

			req := &logical.Request{
				Operation: logical.CreateOperation,
				Path:      path,
				Storage:   storage,
				Data: map[string]interface{}{
					"custom_metadata": initialCustomMetadata,
				},
			}

			resp, err := b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("create request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.PatchOperation,
				Path:      path,
				Storage:   storage,
				Data: map[string]interface{}{
					"custom_metadata": tc.input,
				},
			}

			resp, err = b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("patch request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.ReadOperation,
				Path:      path,
				Storage:   storage,
			}

			resp, err = b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("read request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			var ok bool
			var customMetadata map[string]string

			if customMetadata, ok = resp.Data["custom_metadata"].(map[string]string); !ok {
				t.Fatalf("custom_metadata not included or incorrect type, resp: %#v", resp)
			}

			if diff := deep.Equal(tc.output, customMetadata); len(diff) > 0 {
				t.Fatalf("patched custom metadata does not match, diff: %#v", diff)
			}
		})
	}
}

func TestVersionedKV_Metadata_Patch_Success(t *testing.T) {
	t.Parallel()

	ignoreVal := "ignore_me"
	cases := []struct {
		name            string
		input           map[string]interface{}
		expectedChanges int
	}{
		{
			"ignored_fields",
			map[string]interface{}{
				"foo":             ignoreVal,
				"created_time":    ignoreVal,
				"current_version": ignoreVal,
				"oldest_version":  ignoreVal,
				"updated_time":    ignoreVal,
			},
			0,
		},
		{
			"no_fields_modified",
			map[string]interface{}{},
			0,
		},
		{
			"top_level_fields_replaced",
			map[string]interface{}{
				"cas_required": true,
				"max_versions": uint32(5),
			},
			2,
		},
		{
			"top_level_mixed",
			map[string]interface{}{
				"cas_required":         true,
				"max_versions":         uint32(15),
				"delete_version_after": "0s",
				"updated_time":         ignoreVal,
			},
			3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, storage, events := getBackendWithEvents(t)

			path := "metadata/" + tc.name

			req := &logical.Request{
				Operation: logical.CreateOperation,
				Path:      path,
				Storage:   storage,
				Data: map[string]interface{}{
					"max_versions":         uint32(10),
					"delete_version_after": "10s",
				},
			}

			resp, err := b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("create request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.ReadOperation,
				Path:      path,
				Storage:   storage,
			}

			resp, err = b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("read request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			initialMetadata := resp.Data

			req = &logical.Request{
				Operation: logical.PatchOperation,
				Path:      path,
				Storage:   storage,
				Data:      tc.input,
			}

			resp, err = b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("patch request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.ReadOperation,
				Path:      path,
				Storage:   storage,
			}

			resp, err = b.HandleRequest(context.Background(), req)

			if err != nil || (resp != nil && resp.IsError()) {
				t.Fatalf("read request failed, err: %#v, resp: %#v", err, resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			patchedMetadata := resp.Data

			if diff := deep.Equal(initialMetadata, patchedMetadata); tc.expectedChanges != len(diff) {
				t.Fatalf("incorrect number of changes to metadata, expected: %d, actual: %d, diff: %#v",
					tc.expectedChanges,
					len(diff),
					diff)
			}

			for k, v := range patchedMetadata {
				var expectedVal interface{}

				if inputVal, ok := tc.input[k]; ok && inputVal != nil && inputVal != ignoreVal {
					expectedVal = inputVal
				} else {
					expectedVal = initialMetadata[k]
				}

				if k == "custom_metadata" || k == "versions" {
					if diff := deep.Equal(expectedVal, v); len(diff) > 0 {
						t.Fatalf("patched %q mismatch, diff: %#v", k, diff)
					}
				} else if expectedVal != v {
					t.Fatalf("patched key %s mismatch, expected: %#v, actual %#v", k, expectedVal, v)
				}
			}

			events.expectEvents(t, []expectedEvent{
				{"kv-v2/metadata-write", path, path},
				{"kv-v2/metadata-patch", path, path},
			})
		})
	}
}

func TestVersionedKV_Metadata_Patch_NilsUnset(t *testing.T) {
	b, storage := getBackend(t)

	path := "metadata/nils_unset"

	req := &logical.Request{
		Operation: logical.CreateOperation,
		Path:      path,
		Storage:   storage,
		Data: map[string]interface{}{
			"max_versions": uint32(10),
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("create request failed, err: %#v, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      path,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("read request failed, err: %#v, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if maxVersions := resp.Data["max_versions"].(uint32); maxVersions != 10 {
		t.Fatalf("expected max_versions to be 10")
	}

	req = &logical.Request{
		Operation: logical.PatchOperation,
		Path:      path,
		Storage:   storage,
		Data: map[string]interface{}{
			"max_versions": nil,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("patch request failed, err: %#v, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      path,
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)

	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("read request failed, err: %#v, resp: %#v", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*versionedKVBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	if maxVersions := resp.Data["max_versions"].(uint32); maxVersions != 0 {
		t.Fatalf("expected max_versions to be unset to zero value")
	}
}

func TestVersionedKV_Metadata_List_ExcludeDeleted(t *testing.T) {
	b, storage := getBackend(t)

	// Create first secret "foo"
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

	// Create second secret "bar"
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"test": "value",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/bar",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Create third secret "baz" with multiple versions
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"version": "1",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/baz",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Create second version of "baz"
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"version": "2",
		},
		"options": map[string]interface{}{
			"cas": float64(1),
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/baz",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Test 1: List all secrets (default behavior - exclude_deleted=false)
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	expected := []string{"bar", "baz", "foo"}
	keys := resp.Data["keys"].([]string)
	assertKeysMatch(t, keys, expected, "Test 1: List all secrets (default behavior)")

	// Test 2: List with exclude_deleted=false explicitly
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": false,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys = resp.Data["keys"].([]string)
	expected = []string{"bar", "baz", "foo"}
	assertKeysMatch(t, keys, expected, "Test 2: List with exclude_deleted=false explicitly")

	// Delete the current version of "foo"
	deleteData := map[string]interface{}{
		"versions": "1",
	}

	req = &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "delete/foo",
		Storage:   storage,
		Data:      deleteData,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Test 3: List all secrets after deletion (should still show "foo" by default)
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys = resp.Data["keys"].([]string)
	expected = []string{"bar", "baz", "foo"}
	assertKeysMatch(t, keys, expected, "Test 3: List all secrets after deletion (should still show foo by default)")

	// Test 4: List with exclude_deleted=true (should filter out "foo")
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys = resp.Data["keys"].([]string)
	expected = []string{"bar", "baz"}
	assertKeysMatch(t, keys, expected, "Test 4: List with exclude_deleted=true (should filter out foo)")

	// Test 5: Delete version 1 of "baz"

	deleteData = map[string]interface{}{
		"versions": "1",
	}

	req = &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "delete/baz",
		Storage:   storage,
		Data:      deleteData,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Test 6: List with exclude_deleted=true after deleting old version (should still show "baz") as current version is not deleted
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys = resp.Data["keys"].([]string)
	expected = []string{"bar", "baz"}
	assertKeysMatch(t, keys, expected, "Test 6: List with exclude_deleted=true after deleting old version (should still show baz)")

	// Test 7: Create a directory structure and test filtering with directories
	data = map[string]interface{}{
		"data": map[string]interface{}{
			"nested": "value",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/dir/nested-secret",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Test 8: List root with directories and exclude_deleted=true. Directories should always be included.
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys = resp.Data["keys"].([]string)
	// Should include: bar, baz, dir/ (directories are always included, deleted secrets excluded)
	expected = []string{"bar", "baz", "dir/"}
	assertKeysMatch(t, keys, expected, "Test 8: List root with directories and exclude_deleted=true")
}

func TestVersionedKV_Metadata_List_ExcludeDeleted_EdgeCases(t *testing.T) {
	b, storage := getBackend(t)

	// Test 1: Empty list with exclude_deleted=true
	req := &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true,
		},
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["keys"] != nil {
		t.Fatalf("expected no keys for empty list, got %v", resp.Data["keys"])
	}

	// Test 2: Create a secret and immediately delete its only version
	data := map[string]interface{}{
		"data": map[string]interface{}{
			"key": "value",
		},
	}

	req = &logical.Request{
		Operation: logical.CreateOperation,
		Path:      "data/single-version",
		Storage:   storage,
		Data:      data,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Delete the only version
	deleteData := map[string]interface{}{
		"versions": "1",
	}

	req = &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "delete/single-version",
		Storage:   storage,
		Data:      deleteData,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// List with exclude_deleted=true should not show the secret
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	if resp.Data["keys"] != nil {
		t.Fatalf("expected no keys when only secret is deleted, got %v", resp.Data["keys"])
	}

	// Test 4: List with exclude_deleted=false should still show the secret
	req = &logical.Request{
		Operation: logical.ListOperation,
		Path:      "metadata/",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": false,
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	keys := resp.Data["keys"].([]string)
	expected := []string{"single-version"}
	assertKeysMatch(t, keys, expected, "Edge case: Single entry list with exclude_deleted=true")

	// Test 5: Test metadata read with exclude_deleted (should be ignored for read operations)
	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "metadata/single-version",
		Storage:   storage,
		Data: map[string]interface{}{
			"exclude_deleted": true, // This should be ignored for read operations
		},
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || resp == nil || resp.IsError() {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}

	// Should still return metadata even though current version is deleted
	if resp.Data["current_version"] != uint64(1) {
		t.Fatalf("expected current_version 1, got %v", resp.Data["current_version"])
	}
}
