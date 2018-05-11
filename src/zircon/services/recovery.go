package services

import (
	"zircon/apis"
	"zircon/rpc"
)

func RecoveryService(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	panic("unimplemented")
}
