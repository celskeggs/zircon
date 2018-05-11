package services

import (
	"zircon/apis"
	"zircon/rpc"
)

// Launches cluster services, such as replication and garbage collection.
func StartServices(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {

	// TODO Currently return early on errors, but maybe it's better to still start the other services
	// TODO Clean this up with a list of service function pointers that take the same arguments
	repCancel, err := ReplicatorService(etcd, localCache, rpcCache)
	if err != nil {
		return nil, err
	}
	lbCancel, err := LoadBalancerService(etcd, localCache, rpcCache)
	if err != nil {
		return nil, err
	}
	rcCancel, err := RecoveryService(etcd, localCache, rpcCache)
	if err != nil {
		return nil, err
	}
	gcCancel, err := GCService(etcd, localCache, rpcCache)
	if err != nil {
		return nil, err
	}

	cancel = func() error {
		repErr := repCancel()
		lbErr := lbCancel()
		rcErr := rcCancel()
		gcErr := gcCancel()

		// TODO Combine errors together
		if repErr != nil {
			return repErr
		}
		if lbErr != nil {
			return lbErr
		}
		if rcErr != nil {
			return rcErr
		}
		if gcErr != nil {
			return gcErr
		}

		return nil
	}

	return cancel, nil
}
