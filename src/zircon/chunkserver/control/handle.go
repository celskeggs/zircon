package control

import (
	"zircon/chunkserver/storage"
	"zircon/apis"
)

func ExposeChunkserver(storage storage.ChunkStorage, etcd apis.EtcdInterface) (apis.ChunkserverSingle, error) {
	panic("unimplemented")
}
