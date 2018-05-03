package rpc

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
	"zircon/apis"
)

type ConnectionCache interface {
	// Subscribes to a chunkserver over the network on a specific address.
	// This caches connections.
	// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
	SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error)

	// Subscribes to a frontend RPC server over the network on a specific address.
	// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
	SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error)

	// Subscribes to a metadata cache over the network on a specific address.
	// Failure to connect does *not* cause an error here; just timeouts when trying to call specific methods.
	SubscribeMetadataCache(address apis.ServerAddress) (apis.MetadataCache, error)

	// Closes as many open connections as possible. May disrupt current operations. Should not be necessary to call if
	// no subscriptions have been attempted.
	CloseAll()
}

type conncache struct {
	mu             sync.Mutex
	chunkservers   map[apis.ServerAddress]apis.Chunkserver
	frontends      map[apis.ServerAddress]apis.Frontend
	metadatacaches map[apis.ServerAddress]apis.MetadataCache
	client         *http.Client
	transport      *http.Transport
	closed         bool
}

func NewConnectionCache() ConnectionCache {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{
		Timeout:   time.Second,
		Transport: transport,
	}
	return &conncache{
		client:         client,
		transport:      transport,
		chunkservers:   map[apis.ServerAddress]apis.Chunkserver{},
		frontends:      map[apis.ServerAddress]apis.Frontend{},
		metadatacaches: map[apis.ServerAddress]apis.MetadataCache{},
	}
}

func (c *conncache) SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("attempt to use closed connection cache")
	}

	existingConnection, exists := c.chunkservers[address]
	if exists {
		return existingConnection, nil
	} else {
		newConnection, err := UncachedSubscribeChunkserver(address, c.client)
		if err != nil {
			return nil, err
		}
		c.chunkservers[address] = newConnection
		return newConnection, nil
	}
}

func (c *conncache) SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("attempt to use closed connection cache")
	}

	existingConnection, exists := c.frontends[address]
	if exists {
		return existingConnection, nil
	} else {
		newConnection, err := UncachedSubscribeFrontend(address, c.client)
		if err != nil {
			return nil, err
		}
		c.frontends[address] = newConnection
		return newConnection, nil
	}
}

func (c *conncache) SubscribeMetadataCache(address apis.ServerAddress) (apis.MetadataCache, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("attempt to use closed connection cache")
	}

	existingConnection, exists := c.metadatacaches[address]
	if exists {
		return existingConnection, nil
	} else {
		newConnection, err := UncachedSubscribeMetadataCache(address, c.client)
		if err != nil {
			return nil, err
		}
		c.metadatacaches[address] = newConnection
		return newConnection, nil
	}
}

func (c *conncache) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.transport.CloseIdleConnections()
}
