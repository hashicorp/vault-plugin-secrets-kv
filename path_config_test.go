// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/vault/sdk/framework"
	"github.com/hashicorp/vault/sdk/helper/testhelpers/schema"
	"github.com/hashicorp/vault/sdk/logical"
)

func TestVersionedKV_Config(t *testing.T) {
	b, storage := getBackend(t)

	paths := []*framework.Path{pathConfig(b.(*versionedKVBackend))}

	d := 5 * time.Minute
	data := map[string]interface{}{
		"max_versions":         4,
		"cas_required":         true,
		"delete_version_after": d.String(),
	}

	req := &logical.Request{
		Operation: logical.UpdateOperation,
		Path:      "config",
		Storage:   storage,
		Data:      data,
	}

	resp, err := b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.FindResponseSchema(t, paths, 0, req.Operation),
		resp,
		true,
	)

	req = &logical.Request{
		Operation: logical.ReadOperation,
		Path:      "config",
		Storage:   storage,
	}

	resp, err = b.HandleRequest(context.Background(), req)
	if err != nil || (resp != nil && resp.IsError()) {
		t.Fatalf("err:%s resp:%#v\n", err, resp)
	}
	schema.ValidateResponse(
		t,
		schema.FindResponseSchema(t, paths, 0, req.Operation),
		resp,
		true,
	)

	if resp.Data["max_versions"] != uint32(4) {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["cas_required"] != true {
		t.Fatalf("Bad response: %#v", resp)
	}

	if resp.Data["delete_version_after"] != d.String() {
		t.Fatalf("Bad response: %#v", resp)
	}
}

func getDuration(t *testing.T, in string) time.Duration {
	t.Helper()
	out, err := time.ParseDuration(in)
	if err != nil {
		t.Errorf("ParseDuration(%q) caused err: %#v", in, err)
		return 0
	}
	if out < 0 {
		return disabled
	}
	return out
}

func TestVersionedKV_Config_DeleteVersionAfter(t *testing.T) {
	var tests = []struct {
		ds1, ds2 string
		want     time.Duration
	}{
		{"0s", "0s", 0},
		{"10s", "0s", 0},
		{"10s", "20s", 20 * time.Second},
		{"10s", "-1h", disabled},
		{"-1h", "3h", 3 * time.Hour},
		{"-1h", "-1h", disabled},
		{"-1h", "0h", 0},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("ds1=%v,ds2=%v", tt.ds1, tt.ds2), func(t *testing.T) {
			t.Parallel()

			b, storage := getBackend(t)

			paths := []*framework.Path{pathConfig(b.(*versionedKVBackend))}

			// default value should be 0
			req := &logical.Request{
				Operation: logical.ReadOperation,
				Path:      "config",
				Storage:   storage,
			}
			resp, err := b.HandleRequest(context.Background(), req)
			wantResponse(t, resp, err)
			schema.ValidateResponse(
				t,
				schema.FindResponseSchema(t, paths, 0, req.Operation),
				resp,
				true,
			)

			got := resp.Data["delete_version_after"]
			if got == nil {
				t.Logf("resp: %#v", resp)
				t.Fatal("delete_version_after missing, want the default")
			}

			// set first value
			data := map[string]interface{}{
				"delete_version_after": tt.ds1,
			}
			req = &logical.Request{
				Operation: logical.UpdateOperation,
				Path:      "config",
				Storage:   storage,
				Data:      data,
			}
			resp, err = b.HandleRequest(context.Background(), req)
			wantNoResponse(t, resp, err)
			schema.ValidateResponse(
				t,
				schema.FindResponseSchema(t, paths, 0, req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.ReadOperation,
				Path:      "config",
				Storage:   storage,
			}
			resp, err = b.HandleRequest(context.Background(), req)
			wantResponse(t, resp, err)
			schema.ValidateResponse(
				t,
				schema.FindResponseSchema(t, paths, 0, req.Operation),
				resp,
				true,
			)

			d1 := getDuration(t, tt.ds1)
			want, got := d1.String(), resp.Data["delete_version_after"]
			if want != got {
				t.Logf("resp: %#v", resp)
				t.Fatalf("first value: want delete_version_after: %v, got %v", want, got)
			}

			// set second value
			data = map[string]interface{}{
				"delete_version_after": tt.ds2,
			}
			req = &logical.Request{
				Operation: logical.UpdateOperation,
				Path:      "config",
				Storage:   storage,
				Data:      data,
			}
			resp, err = b.HandleRequest(context.Background(), req)
			wantNoResponse(t, resp, err)
			schema.ValidateResponse(
				t,
				schema.FindResponseSchema(t, paths, 0, req.Operation),
				resp,
				true,
			)

			req = &logical.Request{
				Operation: logical.ReadOperation,
				Path:      "config",
				Storage:   storage,
			}
			resp, err = b.HandleRequest(context.Background(), req)
			wantResponse(t, resp, err)
			schema.ValidateResponse(
				t,
				schema.FindResponseSchema(t, paths, 0, req.Operation),
				resp,
				true,
			)

			want, got = tt.want.String(), resp.Data["delete_version_after"]
			if want != got {
				t.Logf("resp: %#v", resp)
				t.Fatalf("second value: want delete_version_after: %v, got %v", want, got)
			}
		})
	}
}
