package frontend

import "zircon/apis"

// Starts serving an RPC handler for a Frontend on a certain address. Runs forever.
func PublishFrontend(server apis.Frontend, address apis.ServerAddress) error {
	panic("unimplemented")
}

// Subscribes to a frontend RPC server over the network on a specific address.
// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
func SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error) {
	panic("unimplemented")
}

// Constructs a frontend that can connect to the network
func ConstructFrontendOnNetwork(local apis.ServerAddress, etcd apis.EtcdInterface,
		localMetadata apis.MetadataCache) (apis.Frontend, error) {
	panic("unimplemented")
}

// Constructs an interface to a set of frontends as if they were one front-end.
func RoundRobin(servers []apis.Frontend) apis.Frontend {
	panic("unimplemented")
}
