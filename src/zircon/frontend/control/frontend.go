package control

import (
	"zircon/apis"
	"zircon/rpc"
)

// The main module of a frontend, constructed based on an etcd interface, and mechanisms to request a connection to
// metadata subsystems and chunkservers.
func ConstructFrontend(local apis.ServerAddress, etcd apis.EtcdInterface, cache rpc.ConnectionCache) (apis.Frontend, error) {
	panic("unimplemented")
}
