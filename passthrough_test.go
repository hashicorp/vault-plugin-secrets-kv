// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/vault/sdk/helper/testhelpers/snapshots"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/go-secure-stdlib/parseutil"
	"github.com/hashicorp/vault/sdk/helper/testhelpers/schema"
	"github.com/hashicorp/vault/sdk/logical"
)

func testPassthroughBackendWithStorage() (logical.Backend, logical.Storage) {
	storage := &logical.InmemStorage{}
	b, _ := PassthroughBackendFactory(context.Background(), &logical.BackendConfig{
		Logger: nil,
		System: logical.StaticSystemView{
			DefaultLeaseTTLVal: time.Hour * 24,
			MaxLeaseTTLVal:     time.Hour * 24 * 32,
		},
		StorageView: storage,
	})

	return b, storage
}

func TestPassthroughBackend_RootPaths(t *testing.T) {
	b := testPassthroughBackend()
	test := func(b logical.Backend) {
		root := b.SpecialPaths()
		if len(root.Root) != 0 {
			t.Fatalf("unexpected: %v", root)
		}
	}
	test(b)
	b = testPassthroughLeasedBackend()
	test(b)
}

func TestPassthroughBackend_Write(t *testing.T) {
	test := func(b logical.Backend) {
		req := logical.TestRequest(t, logical.UpdateOperation, "foo")
		req.Data["raw"] = "test"

		resp, err := b.HandleRequest(context.Background(), req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if resp != nil {
			t.Fatalf("bad: %v", resp)
		}
		schema.ValidateResponse(
			t,
			schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
			resp,
			true,
		)

		out, err := req.Storage.Get(context.Background(), "foo")
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out == nil {
			t.Fatalf("failed to write to view")
		}
	}
	b := testPassthroughBackend()
	test(b)
	b = testPassthroughLeasedBackend()
	test(b)
}

func TestPassthroughBackend_Read(t *testing.T) {
	test := func(b logical.Backend, ttlType string, ttl interface{}, leased bool) {
		req := logical.TestRequest(t, logical.UpdateOperation, "foo")
		req.Data["raw"] = "test"
		var reqTTL interface{}
		switch ttl.(type) {
		case int64:
			reqTTL = ttl.(int64)
		case string:
			reqTTL = ttl.(string)
		default:
			t.Fatal("unknown ttl type")
		}
		req.Data[ttlType] = reqTTL
		storage := req.Storage

		if _, err := b.HandleRequest(context.Background(), req); err != nil {
			t.Fatalf("err: %v", err)
		}

		req = logical.TestRequest(t, logical.ReadOperation, "foo")
		req.Storage = storage

		resp, err := b.HandleRequest(context.Background(), req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		schema.ValidateResponse(
			t,
			schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
			resp,
			true,
		)

		expectedTTL, err := parseutil.ParseDurationSecond(ttl)
		if err != nil {
			t.Fatal(err)
		}

		// What comes back if an int is passed in is a json.Number which is
		// actually aliased as a string so to make the deep equal happy if it's
		// actually a number we set it to an int64
		var respTTL interface{} = resp.Data[ttlType]
		_, ok := respTTL.(json.Number)
		if ok {
			respTTL, err = respTTL.(json.Number).Int64()
			if err != nil {
				t.Fatal(err)
			}
			resp.Data[ttlType] = respTTL
		}

		expected := &logical.Response{
			Secret: &logical.Secret{
				LeaseOptions: logical.LeaseOptions{
					Renewable: true,
					TTL:       expectedTTL,
				},
			},
			Data: map[string]interface{}{
				"raw":   "test",
				ttlType: reqTTL,
			},
		}

		if !leased {
			expected.Secret.Renewable = false
		}
		resp.Secret.InternalData = nil
		resp.Secret.LeaseID = ""
		if !reflect.DeepEqual(resp, expected) {
			t.Fatalf("bad response.\n\nexpected:\n%#v\n\nGot:\n%#v", expected, resp)
		}
	}
	b := testPassthroughLeasedBackend()
	test(b, "lease", "1h", true)
	test(b, "ttl", "5", true)
	b = testPassthroughBackend()
	test(b, "lease", int64(10), false)
	test(b, "ttl", "40s", false)
}

func TestPassthroughBackend_Delete(t *testing.T) {
	for name, f := range map[string]func(*mockEventsSender) logical.Backend{
		"no lease": testPassthroughBackendWithEvents,
		"leased":   testPassthroughLeasedBackendWithEvents,
	} {
		t.Run(name, func(t *testing.T) {
			events := &mockEventsSender{}
			b := f(events)
			req := logical.TestRequest(t, logical.UpdateOperation, "foo")
			req.Data["raw"] = "test"
			storage := req.Storage

			if _, err := b.HandleRequest(context.Background(), req); err != nil {
				t.Fatalf("err: %v", err)
			}

			req = logical.TestRequest(t, logical.DeleteOperation, "foo")
			req.Storage = storage
			resp, err := b.HandleRequest(context.Background(), req)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if resp != nil {
				t.Fatalf("bad: %v", resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)

			req = logical.TestRequest(t, logical.ReadOperation, "foo")
			req.Storage = storage
			resp, err = b.HandleRequest(context.Background(), req)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if resp != nil {
				t.Fatalf("bad: %v", resp)
			}
			schema.ValidateResponse(
				t,
				schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
				resp,
				true,
			)
			events.expectEvents(t, []expectedEvent{
				{"kv-v1/write", "foo", "foo"},
				{"kv-v1/delete", "foo", ""},
			})
		})
	}
}

func TestPassthroughBackend_List(t *testing.T) {
	test := func(b logical.Backend) {
		req := logical.TestRequest(t, logical.UpdateOperation, "foo")
		req.Data["raw"] = "test"
		storage := req.Storage

		if _, err := b.HandleRequest(context.Background(), req); err != nil {
			t.Fatalf("err: %v", err)
		}

		req = logical.TestRequest(t, logical.ListOperation, "")
		req.Storage = storage
		resp, err := b.HandleRequest(context.Background(), req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		schema.ValidateResponse(
			t,
			schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
			resp,
			true,
		)

		expected := &logical.Response{
			Data: map[string]interface{}{
				"keys": []string{"foo"},
			},
		}

		if !reflect.DeepEqual(resp, expected) {
			t.Fatalf("bad response.\n\nexpected: %#v\n\nGot: %#v", expected, resp)
		}
	}
	b := testPassthroughBackend()
	test(b)
	b = testPassthroughLeasedBackend()
	test(b)
}

func TestPassthroughBackend_Revoke(t *testing.T) {
	test := func(b logical.Backend) {
		req := logical.TestRequest(t, logical.RevokeOperation, "kv")
		req.Secret = &logical.Secret{
			InternalData: map[string]interface{}{
				"secret_type": "kv",
			},
		}

		if _, err := b.HandleRequest(context.Background(), req); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	b := testPassthroughBackend()
	test(b)
	b = testPassthroughLeasedBackend()
	test(b)
}

func TestPassthroughBackend_Renew(t *testing.T) {
	b := testPassthroughLeasedBackend()

	req := logical.TestRequest(t, logical.CreateOperation, "foo")
	req.Data = map[string]interface{}{
		"ttl":     "4h",
		"payload": "alpha",
	}
	storage := req.Storage
	if _, err := b.HandleRequest(context.Background(), req); err != nil {
		t.Fatalf("err: %v", err)
	}

	req = logical.TestRequest(t, logical.RenewOperation, "foo")
	req.Storage = storage
	req.Secret = &logical.Secret{
		InternalData: map[string]interface{}{
			"secret_type": "kv",
		},
	}
	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := map[string]interface{}{
		"ttl":     "4h",
		"payload": "alpha",
	}
	if !reflect.DeepEqual(resp.Data, expected) {
		t.Fatalf("bad response.\n\nexpected: %#v\n\nGot: %#v", expected, resp)
	}
}

func TestPassthroughBackend_Recover(t *testing.T) {
	config := logical.TestBackendConfig()
	config.StorageView = &logical.InmemStorage{}
	b, err := LeasedPassthroughBackendFactory(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	err = b.Setup(context.Background(), config)

	oldConfig := logical.TestBackendConfig()
	oldConfig.StorageView = &logical.InmemStorage{}
	oldBackend, err := LeasedPassthroughBackendFactory(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	err = oldBackend.Setup(context.Background(), oldConfig)
	tc := snapshots.NewSnapshotTestCase(t, b)
	snapshotStorage := tc.SnapshotStorage()
	regularStorage := tc.RegularStorage()
	if snapshotStorage == nil {
		t.Fatalf("not nil; snapshotStorage")
	}
	if regularStorage == nil {
		t.Fatalf("not nil; regularStorage")
	}

	// write a KV entry
	req := logical.TestRequest(t, logical.UpdateOperation, "foo")
	req.Data["raw"] = "test"
	req.Storage = snapshotStorage
	resp, err := oldBackend.HandleRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp != nil {
		t.Fatalf("bad: %v", resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	// verify that read and list from a snapshot don't modify anything
	tc.RunRead(t, "foo")
	tc.RunList(t, "foo")

	// run a recover
	_, err = tc.DoRecover(t, "foo")

	// check after the recover operation to ensure the data was updated
	read, err := b.HandleRequest(context.Background(), &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "foo",
		Storage:   regularStorage,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if read == nil {
		t.Fatalf("bad: %v", read)
	}
	if read.Data["raw"] != "test" {
		t.Fatalf("bad %v", read.Data["raw"])
	}

	// modify the entry in the regular storage
	req = logical.TestRequest(t, logical.UpdateOperation, "foo")
	req.Data["raw"] = "test_two"
	req.Storage = regularStorage
	resp, err = oldBackend.HandleRequest(context.Background(), req)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if resp != nil {
		t.Fatalf("bad: %v", resp)
	}
	schema.ValidateResponse(
		t,
		schema.GetResponseSchema(t, b.(*PassthroughBackend).Route(req.Path), req.Operation),
		resp,
		true,
	)

	// check after modifying to ensure the data was updated
	read, err = b.HandleRequest(context.Background(), &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "foo",
		Storage:   regularStorage,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if read == nil {
		t.Fatalf("bad: %v", read)
	}
	if read.Data["raw"] != "test_two" {
		t.Fatalf("bad %v", read.Data["raw"])
	}

	// run a recover again
	_, err = tc.DoRecover(t, "foo")

	// check after the recover operation to ensure the data was updated
	read, err = b.HandleRequest(context.Background(), &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "foo",
		Storage:   regularStorage,
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if read == nil {
		t.Fatalf("bad: %v", read)
	}
	if read.Data["raw"] != "test" {
		t.Fatalf("bad %v", read.Data["raw"])
	}

}

func testPassthroughBackend() logical.Backend {
	return testPassthroughBackendWithEvents(nil)
}

func testPassthroughLeasedBackend() logical.Backend {
	return testPassthroughLeasedBackendWithEvents(nil)
}

func testPassthroughBackendWithEvents(events *mockEventsSender) logical.Backend {
	b, _ := PassthroughBackendFactory(context.Background(), &logical.BackendConfig{
		Logger: nil,
		System: logical.StaticSystemView{
			DefaultLeaseTTLVal: time.Hour * 24,
			MaxLeaseTTLVal:     time.Hour * 24 * 32,
		},
		EventsSender: events,
	})
	return b
}

func testPassthroughLeasedBackendWithEvents(events *mockEventsSender) logical.Backend {
	b, _ := LeasedPassthroughBackendFactory(context.Background(), &logical.BackendConfig{
		Logger: nil,
		System: logical.StaticSystemView{
			DefaultLeaseTTLVal: time.Hour * 24,
			MaxLeaseTTLVal:     time.Hour * 24 * 32,
		},
		EventsSender: events,
	})
	return b
}
