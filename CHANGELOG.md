## Unreleased
## 0.16.1

CHANGES:

* Updated dependencies:
  * `golang.org/x/crypto` v0.6.0 -> v0.12.0
  * `golang.org/x/net` v0.8.0 -> v0.14.0
  * `golang.org/x/text` v0.8.0 -> v0.12.0
## 0.16.0

CHANGES:

* Events: now include `data_path`, `operation`, and `modified` [GH-124](https://github.com/hashicorp/vault-plugin-secrets-kv/pull/124)
* Updated dependencies:
   * `github.com/hashicorp/vault/api` v1.9.0 -> v1.9.2
   * `github.com/hashicorp/vault/sdk` v0.9.0 -> v0.9.3-0.20230831152851-56ce89544e64
   * `google.golang.org/protobuf` v1.30.0 ->  v1.31.0

## 0.15.0

IMPROVEMENTS:

* Add display attributes for OpenAPI OperationID's [GH-104](https://github.com/hashicorp/vault-plugin-secrets-kv/pull/104)
* Add versions to delete and undelete events [GH-122](https://github.com/hashicorp/vault-plugin-secrets-kv/pull/122)
