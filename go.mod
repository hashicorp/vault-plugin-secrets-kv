module github.com/hashicorp/vault-plugin-secrets-kv

go 1.16

replace github.com/hashicorp/vault/sdk => ../vault-enterprise/sdk

require (
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/Masterminds/vcs v1.13.3 // indirect
	github.com/boltdb/bolt v1.3.1 // indirect
	github.com/go-test/deep v1.0.8
	github.com/golang/dep v0.5.4 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v1.3.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/go-plugin v1.4.5 // indirect
	github.com/hashicorp/go-secure-stdlib/parseutil v0.1.7
	github.com/hashicorp/go-secure-stdlib/strutil v0.1.2
	github.com/hashicorp/go-version v1.6.0 // indirect
	github.com/hashicorp/vault/api v1.8.0
	github.com/hashicorp/vault/sdk v0.8.1
	github.com/hashicorp/yamux v0.0.0-20181012175058-2f1d1f20f75d // indirect
	github.com/jmank88/nuts v0.4.0 // indirect
	github.com/mitchellh/gox v1.0.1 // indirect
	github.com/mitchellh/mapstructure v1.5.0
	github.com/nightlyone/lockfile v1.0.0 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/sdboyer/constext v0.0.0-20170321163424-836a14457353 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	google.golang.org/protobuf v1.28.1
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
