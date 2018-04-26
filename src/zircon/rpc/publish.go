package rpc

import (
	"zircon/apis"
	"zircon/chunkserver/control"
)

type ConnectionCache interface {
	// Subscribes to a chunkserver over the network on a specific address.
	// This caches connections.
	// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
	SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error)

	// Subscribes to a frontend RPC server over the network on a specific address.
	// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
	SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error)

	CloseAll()
}

func NewConnectionCache() (ConnectionCache, error) {
	panic("unimplemented")
}

// Starts serving an RPC handler for a Chunkserver on a certain address. Runs forever.
func PublishChunkserver(server apis.Chunkserver, address string) (control.Teardown, apis.ServerAddress, error) {
	panic("unimplemented")
}

// Starts serving an RPC handler for a Frontend on a certain address. Runs forever.
func PublishFrontend(server apis.Frontend, address apis.ServerAddress) error {
	panic("unimplemented")
}
