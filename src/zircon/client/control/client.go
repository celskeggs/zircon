package control

import "zircon/apis"

// Construct a client handler that can provide the apis.Client interface based on a single frontend and a way to connect
// to chunkservers.
// (Note: this frontend will likely be a zircon.frontend.RoundRobin implementation in most cases.)
func ConstructClient(frontend apis.Frontend, chunkservers func(address apis.ServerAddress) apis.Chunkserver) (apis.Client, error) {
	panic("unimplemented")
}
