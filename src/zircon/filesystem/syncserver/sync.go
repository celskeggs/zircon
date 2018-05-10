package syncserver

import (
	"zircon/apis"
	"zircon/rpc"
)

func NewSyncServer(etcd apis.EtcdInterface) apis.SyncServer {
	return etcd
}

func ConstructAndPublish(etcd apis.EtcdInterface, address apis.ServerAddress) (func(bool) error, apis.ServerAddress, error) {
	return rpc.PublishSyncServer(NewSyncServer(etcd), address)
}
