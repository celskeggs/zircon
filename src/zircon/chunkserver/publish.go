package chunkserver

import "zircon/apis"

// Starts serving an RPC handler for a Chunkserver on a certain address. Runs forever.
func PublishChunkserver(server apis.Chunkserver, address string) error {
	panic("unimplemented")
}

// Supplement a basic chunkserver interface with the ability to connect to other chunkservers
func WithChatter(server apis.ChunkserverSingle) (apis.Chunkserver, error) {
	panic("unimplemented")
}

// Subscribes to a chunkserver over the network on a specific address.
// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
func SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error) {
	panic("unimplemented")
}
