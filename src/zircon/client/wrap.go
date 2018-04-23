package client

import "zircon/apis"

// The configuration information provided by a client application to connect to a Zircon cluster.
type Configuration struct {
	// TODO
}

// Set up all portions of a client based on a Zircon configuration.
// This will not error if servers aren't available; timeout errors will occur when methods on the client are invoked.
func ConfigureClient(config *Configuration) (apis.Client, error) {
	panic("unimplemented")
}
