package rpc

import (
	"net/http"
	"zircon/apis"
)

// Connects to an RPC handler for a Frontend on a certain address.
func UncachedSubscribeFrontend(address apis.ServerAddress, client *http.Client) (apis.Frontend, error) {
	panic("unimplemented")
}

// Starts serving an RPC handler for a Frontend on a certain address. Runs forever.
func PublishFrontend(server apis.Frontend, address apis.ServerAddress) error {
	panic("unimplemented")
}
