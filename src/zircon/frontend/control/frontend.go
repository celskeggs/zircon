package control

import "zircon/apis"

func ConstructFrontend(local apis.ServerAddress, etcd apis.EtcdInterface,
		connectMetadata func(address apis.ServerAddress) (apis.MetadataCache, error),
		connectChunkserver func(address apis.ServerAddress) (apis.Chunkserver, error)) (apis.Frontend, error) {
	panic("unimplemented")
}
