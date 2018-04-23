package control

import (
	"zircon/chunkserver/storage"
	"zircon/apis"
)

// This includes most of the chunkserver implementation; which it exports through the ChunkserverSingle interface, based
// on a storage layer and a connection to etcd.
func ExposeChunkserver(storage storage.ChunkStorage, etcd apis.EtcdInterface) (apis.ChunkserverSingle, error) {
	panic("unimplemented")
}
