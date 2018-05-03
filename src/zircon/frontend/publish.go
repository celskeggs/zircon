package frontend

import (
	"zircon/apis"
	"zircon/rpc"
	"errors"
)

// Constructs a frontend that can connect to the network
func ConstructFrontendOnNetwork(etcd apis.EtcdInterface, cache rpc.ConnectionCache) (apis.Frontend, error) {
	return nil, errors.New("ConstructFrontendOnNetwork is unimplemented")
}
