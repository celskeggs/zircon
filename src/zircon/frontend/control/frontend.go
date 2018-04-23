package control

import "zircon/apis"

// The main module of a frontend, constructed based on an etcd interface, and mechanisms to request a connection to
// metadata subsystems and chunkservers.
func ConstructFrontend(local apis.ServerAddress, etcd apis.EtcdInterface,
		connectMetadata func(address apis.ServerAddress) (apis.MetadataCache, error),
		connectChunkserver func(address apis.ServerAddress) (apis.Chunkserver, error)) (apis.Frontend, error) {
	panic("unimplemented")
}
