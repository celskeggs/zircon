package frontend

import (
	"zircon/apis"
	"zircon/rpc"
	"github.com/pkg/errors"
)

// Constructs a frontend that can connect to the network
func ConstructFrontendOnNetwork(local apis.ServerAddress, etcd apis.EtcdInterface, cache rpc.ConnectionCache) (apis.Frontend, error) {
	return nil, errors.New("ConstructFrontendOnNetwork is unimplemented")
}
