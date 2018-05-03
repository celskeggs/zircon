package client

import (
	"errors"
	"zircon/apis"
	"zircon/client/control"
	"zircon/frontend"
	"zircon/rpc"
)

// The configuration information provided by a client application to connect to a Zircon cluster.
type Configuration struct {
	FrontendAddresses []apis.ServerAddress
}

// Set up all portions of a client based on a Zircon configuration.
// This will not error if servers aren't available; timeout errors will occur when methods on the client are invoked.
func ConfigureClient(config Configuration, cache rpc.ConnectionCache) (apis.Client, error) {
	if len(config.FrontendAddresses) < 1 {
		return nil, errors.New("not enough frontend addresses for client")
	}
	frontends := make([]apis.Frontend, len(config.FrontendAddresses))
	var err error
	for i, address := range config.FrontendAddresses {
		frontends[i], err = cache.SubscribeFrontend(address)
		if err != nil {
			return nil, err
		}
	}
	roundrobin := frontend.RoundRobin(frontends)
	return control.ConstructClient(roundrobin, cache)
}

func ConfigureNetworkedClient(config Configuration) (apis.Client, error) {
	cache := rpc.NewConnectionCache()
	client, err := ConfigureClient(config, cache)
	if err != nil {
		cache.CloseAll()
		return nil, err
	}
	return &clientWithCloseCallback{
		base: client,
		close: cache.CloseAll,
	}, nil
}

type clientWithCloseCallback struct {
	base  apis.Client
	close func()
}

func (c *clientWithCloseCallback) New() (apis.ChunkNum, error) {
	return c.base.New()
}

func (c *clientWithCloseCallback) Read(ref apis.ChunkNum, offset apis.Offset, length apis.Length) ([]byte, apis.Version, error) {
	return c.base.Read(ref, offset, length)
}

func (c *clientWithCloseCallback) Write(ref apis.ChunkNum, offset apis.Offset, version apis.Version, data []byte) (apis.Version, error) {
	return c.base.Write(ref, offset, version, data)
}

func (c *clientWithCloseCallback) Delete(ref apis.ChunkNum, version apis.Version) error {
	return c.base.Delete(ref, version)
}

func (c *clientWithCloseCallback) Close() error {
	err := c.base.Close()
	c.close()
	return err
}