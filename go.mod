module github.com/hashicorp/vault-plugin-secrets-kv

go 1.16

replace github.com/hashicorp/vault/sdk => ../vault/sdk

require (
	github.com/evanphx/json-patch/v5 v5.5.0
	github.com/go-test/deep v1.0.7
	github.com/golang/protobuf v1.5.0
	github.com/hashicorp/go-hclog v0.16.2
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-secure-stdlib/parseutil v0.1.1
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.1
	github.com/hashicorp/go-version v1.3.0 // indirect
	github.com/hashicorp/vault/api v1.0.5-0.20200215224050-f6547fa8e820
	github.com/hashicorp/vault/sdk v0.1.14-0.20200215224050-f6547fa8e820
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/mitchellh/mapstructure v1.4.1
	google.golang.org/protobuf v1.27.1
)
