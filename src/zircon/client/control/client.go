package control

import (
	"zircon/apis"
	"zircon/rpc"
	"errors"
)

// Construct a client handler that can provide the apis.Client interface based on a single frontend and a way to connect
// to chunkservers.
// (Note: this frontend will likely be a zircon.frontend.RoundRobin implementation in most cases.)
func ConstructClient(frontend apis.Frontend, conncache rpc.ConnectionCache) (apis.Client, error) {
	return nil, errors.New("ConstructClient is unimplemented")
}
