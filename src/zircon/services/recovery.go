package services

import (
	"zircon/apis"
	"zircon/rpc"
)

// TODO This whole thing
func RecoveryService(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	cancel = func() error {
		return nil
	}

	return cancel, nil
}
