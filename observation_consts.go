// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package kv

const (
	ObservationTypeKVv1SecretRead   = "kvv1/secret/read"
	ObservationTypeKVv1SecretWrite  = "kvv1/secret/write"
	ObservationTypeKVv1SecretDelete = "kvv1/secret/delete"

	ObservationTypeKVv2SecretRead     = "kvv2/secret/read"
	ObservationTypeKVv2SecretWrite    = "kvv2/secret/write"
	ObservationTypeKVv2SecretDelete   = "kvv2/secret/delete"
	ObservationTypeKVv2SecretUndelete = "kvv2/secret/undelete"
	ObservationTypeKVv2SecretDestroy  = "kvv2/secret/destroy"
	ObservationTypeKVv2SecretPatch    = "kvv2/secret/patch"
	ObservationTypeKVv2ConfigRead     = "kvv2/config/read"
	ObservationTypeKVv2ConfigWrite    = "kvv2/config/write"
	ObservationTypeKVv2MetadataRead   = "kvv2/metadata/read"
	ObservationTypeKVv2MetadataWrite  = "kvv2/metadata/write"
	ObservationTypeKVv2MetadataDelete = "kvv2/metadata/delete"
	ObservationTypeKVv2MetadataPatch  = "kvv2/metadata/patch"
)
