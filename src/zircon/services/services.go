package services

import (
	"zircon/apis"
	"zircon/rpc"
	"github.com/pkg/errors"
)

// Launches cluster services, such as replication and garbage collection.
func StartServices(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	return nil, errors.New("unimplemented")
}

// Explanation of the replication service:
//     Every chunk in the cluster should be replicated to at least two servers, preferably three.
//     The replication service goes through, counts valid replicas, and replicates new ones as necessary.
//         (TODO: have chunkservers periodically check their disk checksums)
// Explanation of the garbage collection service:
//     The garbage collection service goes through and finds chunkservers that only have old versions of chunks, such as
//     if a write was performed during a network partition or while a server was down, and then deletes these old and
//     useless chunks.
