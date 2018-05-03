package rpc

import (
	"zircon/apis"
	"errors"
	"net/http"
)

// Connects to an RPC handler for a MetadataCache on a certain address.
func UncachedSubscribeMetadataCache(address apis.ServerAddress, client *http.Client) (apis.MetadataCache, error) {
	return nil, errors.New("unimplemented")
}

// Starts serving an RPC handler for a MetadataCache on a certain address. Runs forever.
func PublishMetadataCache(server apis.MetadataCache, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	return nil, "", errors.New("unimplemented")
}
