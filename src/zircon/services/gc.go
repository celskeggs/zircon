package services

import (
	"zircon/apis"
	"zircon/rpc"
)

// Explanation of the garbage collection service:
//     The garbage collection service goes through and finds chunkservers that only have old versions of chunks, such as
//     if a write was performed during a network partition or while a server was down, and then deletes these old and
//     useless chunks.
func GCService(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	panic("unimplemented")
}
