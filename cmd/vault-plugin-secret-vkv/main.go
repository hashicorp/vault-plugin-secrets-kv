package main

import (
	"os"

	log "github.com/mgutz/logxi/v1"

	"github.com/hashicorp/vault-plugin-secret-vkv"
	"github.com/hashicorp/vault/helper/pluginutil"
	"github.com/hashicorp/vault/logical/plugin"
)

func main() {
	apiClientMeta := &pluginutil.APIClientMeta{}
	flags := apiClientMeta.FlagSet()
	flags.Parse(os.Args[1:])

	tlsConfig := apiClientMeta.GetTLSConfig()
	tlsProviderFunc := pluginutil.VaultPluginTLSProvider(tlsConfig)

	os.Setenv("VAULT_VERSION", "0.9.4")

	err := plugin.Serve(&plugin.ServeOpts{
		BackendFactoryFunc: vkv.Factory,
		TLSProviderFunc:    tlsProviderFunc,
	})
	if err != nil {
		log.Error("plugin shutting down", "error", err)
		os.Exit(1)
	}
}
