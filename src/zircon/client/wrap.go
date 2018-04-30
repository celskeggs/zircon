package client

import (
	"errors"
	"zircon/apis"
	"zircon/client/control"
	"zircon/frontend"
	"zircon/rpc"
)

// The configuration information provided by a client application to connect to a Zircon cluster.
type Configuration struct {
	FrontendAddresses []apis.ServerAddress
}

// Set up all portions of a client based on a Zircon configuration.
// This will not error if servers aren't available; timeout errors will occur when methods on the client are invoked.
func ConfigureClient(config *Configuration, cache rpc.ConnectionCache) (apis.Client, error) {
	if len(config.FrontendAddresses) < 1 {
		return nil, errors.New("not enough frontend addresses for client")
	}
	frontends := make([]apis.Frontend, len(config.FrontendAddresses))
	var err error
	for i, address := range config.FrontendAddresses {
		frontends[i], err = cache.SubscribeFrontend(address)
		if err != nil {
			return nil, err
		}
	}
	roundrobin := frontend.RoundRobin(frontends)
	return control.ConstructClient(roundrobin, cache)
}
