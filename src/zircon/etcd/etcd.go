package etcd

import (
	"zircon/apis"
	"github.com/coreos/etcd/clientv3"
)

// Provides our specific etcd interface based on a connection to etcd
func AcceptEtcd(etcd clientv3.Client) (apis.EtcdInterface, error) {
	panic("unimplemented")
}

// Connects to etcd, as preparation for AcceptEtcd()
func SubscribeEtcd(servers []apis.ServerAddress) (clientv3.Client, error) {
	panic("unimplemented")
}

// Constructs a mock interface to etcd, for the sake of testing.
func MockEtcdInterface() (apis.EtcdInterface, error) {
	panic("unimplemented")
}