package rpc

import (
	"fmt"
	"zircon/apis"
)

type MockCache struct {
	Frontends      map[apis.ServerAddress]apis.Frontend
	Chunkservers   map[apis.ServerAddress]apis.Chunkserver
	MetadataCaches map[apis.ServerAddress]apis.MetadataCache
	SyncServers    map[apis.ServerAddress]apis.SyncServer
}

var _ ConnectionCache = &MockCache{}

func (mc *MockCache) SubscribeChunkserver(address apis.ServerAddress) (apis.Chunkserver, error) {
	cs, found := mc.Chunkservers[address]
	if found {
		return cs, nil
	} else {
		return nil, fmt.Errorf("no such chunkserver: %s", address)
	}
}

func (mc *MockCache) SubscribeFrontend(address apis.ServerAddress) (apis.Frontend, error) {
	fe, found := mc.Frontends[address]
	if found {
		return fe, nil
	} else {
		return nil, fmt.Errorf("no such frontend: %s", address)
	}
}

func (mc *MockCache) SubscribeMetadataCache(address apis.ServerAddress) (apis.MetadataCache, error) {
	mdc, found := mc.MetadataCaches[address]
	if found {
		return mdc, nil
	} else {
		return nil, fmt.Errorf("no such metadata cache: %s", address)
	}
}

func (mc *MockCache) SubscribeSyncServer(address apis.ServerAddress) (apis.SyncServer, error) {
	ss, found := mc.SyncServers[address]
	if found {
		return ss, nil
	} else {
		return nil, fmt.Errorf("no such sync server: %s", address)
	}
}

func (mc *MockCache) CloseAll() {
	// don't bother doing anything
}
