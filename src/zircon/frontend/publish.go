package frontend

import "zircon/apis"

// Constructs a frontend that can connect to the network
func ConstructFrontendOnNetwork(local apis.ServerAddress, etcd apis.EtcdInterface,
		localMetadata apis.MetadataCache) (apis.Frontend, error) {
	panic("unimplemented")
}

// Constructs an interface to a set of frontends as if they were one front-end.
func RoundRobin(servers []apis.Frontend) apis.Frontend {
	panic("unimplemented")
}
