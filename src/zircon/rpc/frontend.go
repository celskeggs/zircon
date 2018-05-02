package rpc

import (
	"net/http"
	"zircon/apis"
	"github.com/pkg/errors"
)

// Connects to an RPC handler for a Frontend on a certain address.
func UncachedSubscribeFrontend(address apis.ServerAddress, client *http.Client) (apis.Frontend, error) {
	return nil, errors.New("unimplemented")
}

// Starts serving an RPC handler for a Frontend on a certain address. Runs forever.
func PublishFrontend(server apis.Frontend, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	return nil, "", errors.New("unimplemented")
}
