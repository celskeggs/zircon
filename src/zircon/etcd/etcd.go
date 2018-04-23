package etcd

import (
	"zircon/apis"
	"github.com/coreos/etcd/clientv3"
)

func AcceptEtcd(etcd clientv3.Client) (apis.EtcdInterface, error) {
	panic("unimplemented")
}

func SubscribeEtcd(servers []apis.ServerAddress) (clientv3.Client, error) {
	panic("unimplemented")
}
