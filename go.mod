module github.com/hashicorp/vault-plugin-secrets-kv

go 1.12

replace github.com/hashicorp/vault/sdk => ../vault-enterprise/sdk

require (
	github.com/armon/go-metrics v0.3.7 // indirect
	github.com/golang/protobuf v1.4.2
	github.com/hashicorp/go-hclog v0.16.2
	github.com/hashicorp/go-secure-stdlib/parseutil v0.1.1
	github.com/hashicorp/vault/api v1.0.5-0.20200215224050-f6547fa8e820
	github.com/hashicorp/vault/sdk v0.1.14-0.20200215224050-f6547fa8e820
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1
)
