package control

import (
	"zircon/apis"
	"zircon/rpc"
	"zircon/chunkupdate"
	"fmt"
)

type reselectingMetadataUpdater struct {
	etcd  apis.EtcdInterface
	cache rpc.ConnectionCache
}

var _ chunkupdate.UpdaterMetadata = &reselectingMetadataUpdater{}

// TODO: avoid inefficiently rerequesting access to the same metadata caches...
// (though these *are* cached by the RPC connectionCache, so it shouldn't be completely horrible)
func (r *reselectingMetadataUpdater) getMetadataCache() (apis.MetadataCache, error) {
	// connect to the local metadata cache
	address, err := r.etcd.GetAddress(r.etcd.GetName(), apis.METADATACACHE)
	if err != nil {
		return nil, fmt.Errorf("each frontend must have a local metadata cache, but: %v", err)
	}
	cache, err := r.cache.SubscribeMetadataCache(address)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func (r *reselectingMetadataUpdater) getSpecificMetadataCache(redirect apis.ServerName) (apis.MetadataCache, error) {
	// connect to the local metadata cache
	address, err := r.etcd.GetAddress(redirect, apis.METADATACACHE)
	if err != nil {
		return nil, fmt.Errorf("cannot find target of redirection: %v", err)
	}
	cache, err := r.cache.SubscribeMetadataCache(address)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

const MaxRedirections = 30

func (r *reselectingMetadataUpdater) runRedirectionLoop(attempt func(apis.MetadataCache) (apis.ServerName, error)) error {
	cache, err := r.getMetadataCache()
	if err != nil {
		return err
	}
	var lastSkippedError error
	for tries := 0; tries < MaxRedirections; tries++ {
		redirect, err := attempt(cache)
		if err == nil {
			return nil
		} else if redirect == apis.NO_REDIRECT {
			return err
		} else {
			lastSkippedError = err
			cache, err = r.getSpecificMetadataCache(redirect)
			if err != nil {
				return err
			}
			// fall through; let's try this again with the correct server.
		}
	}
	// ran out of attempts to redirect to the correct server. probably a redirection loop!
	err = fmt.Errorf("probable redirection loop; original error: %v", lastSkippedError)
	return err
}

func (r *reselectingMetadataUpdater) NewEntry() (apis.ChunkNum, error) {
	cache, err := r.getMetadataCache()
	if err != nil {
		return 0, err
	}
	return cache.NewEntry()
}

func (r *reselectingMetadataUpdater) ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, error) {
	var entry apis.MetadataEntry
	err := r.runRedirectionLoop(func(cache apis.MetadataCache) (redirect apis.ServerName, err error) {
		entry, redirect, err = cache.ReadEntry(chunk)
		return
	})
	return entry, err
}

func (r *reselectingMetadataUpdater) UpdateEntry(chunk apis.ChunkNum, previous apis.MetadataEntry, next apis.MetadataEntry) error {
	return r.runRedirectionLoop(func(cache apis.MetadataCache) (apis.ServerName, error) {
		return cache.UpdateEntry(chunk, previous, next)
	})
}

func (r *reselectingMetadataUpdater) DeleteEntry(chunk apis.ChunkNum, previous apis.MetadataEntry) error {
	return r.runRedirectionLoop(func(cache apis.MetadataCache) (apis.ServerName, error) {
		return cache.DeleteEntry(chunk, previous)
	})
}
