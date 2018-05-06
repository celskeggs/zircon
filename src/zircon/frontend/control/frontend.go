package control

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"zircon/apis"
	"zircon/rpc"
)

const InitialReplicationFactor = 2

type frontend struct {
	etcd  apis.EtcdInterface
	cache rpc.ConnectionCache

	mu       sync.Mutex
	metadata apis.MetadataCache
}

// Construct a frontend server, not including metadata caches and service handlers.
func ConstructFrontend(local apis.ServerAddress, etcd apis.EtcdInterface, cache rpc.ConnectionCache) (apis.Frontend, error) {
	err := etcd.UpdateAddress(local, apis.FRONTEND)
	if err != nil {
		return nil, err
	}
	return &frontend{}, nil
}

func (f *frontend) getMetadataCache() (apis.MetadataCache, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.metadata != nil {
		return f.metadata, nil
	}
	// if possible, connect to a local metadata cache
	address, err := f.etcd.GetAddress(f.etcd.GetName(), apis.METADATACACHE)
	if err != nil {
		options, err := f.etcd.ListServers(apis.METADATACACHE)
		if err != nil {
			return nil, err
		}
		// select an arbitrary server, which is hopefully different from other frontends' selections
		myId, err := f.etcd.GetIDByName(f.etcd.GetName())
		if err != nil {
			return nil, err
		}
		chosen := options[int(myId)%len(options)]
		// this shouldn't fail, given that ListServers returned this...
		address, err = f.etcd.GetAddress(chosen, apis.METADATACACHE)
		if err != nil {
			return nil, err
		}
	}
	// TODO: automatically try a different metadata cache if the current one seems to be missing
	cache, err := f.cache.SubscribeMetadataCache(address)
	if err != nil {
		return nil, err
	}
	f.metadata = cache
	return cache, nil
}

func (f *frontend) selectInitialChunkservers() ([]apis.ServerID, error) {
	chunkservers, err := f.etcd.ListServers(apis.CHUNKSERVER)
	if err != nil {
		return nil, err
	}
	if len(chunkservers) < InitialReplicationFactor {
		// TODO: make sure that old chunkservers are autoremoved
		return nil, errors.New("cannot create new chunks: not enough chunkservers available")
	}
	result := make([]apis.ServerID, InitialReplicationFactor)
	for i, ii := range rand.Perm(len(chunkservers))[:InitialReplicationFactor] {
		id, err := f.etcd.GetIDByName(chunkservers[ii])
		if err != nil {
			return nil, err
		}
		result[i] = id
	}
	return result, nil
}

// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
// with a version of AnyVersion.
// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
func (f *frontend) New() (apis.ChunkNum, error) {
	mc, err := f.getMetadataCache()
	if err != nil {
		return 0, err
	}
	// TODO: try to load-balance when initially selecting chunkservers
	replicas, err := f.selectInitialChunkservers()
	if err != nil {
		return 0, err
	}
	// TODO: garbage collection should look for Version=0 metadata entries and delete them
	chunk, err := mc.NewEntry()
	if err != nil {
		return 0, err
	}
	err = mc.UpdateEntry(chunk, apis.MetadataEntry{}, apis.MetadataEntry{
		MostRecentVersion:   0,
		LastConsumedVersion: 0,
		Replicas:            replicas,
	})
	// TODO: how does garbage collection know not to delete this until the client disconnects early or this server crashes?
	if err != nil {
		// oh well, it'll get cleaned up by garbage collection
		return 0, err
	}
	return chunk, nil
}

func (f *frontend) getReplicaAddresses(entry apis.MetadataEntry) ([]apis.ServerAddress, error) {
	addresses := make([]apis.ServerAddress, len(entry.Replicas))
	for i, id := range entry.Replicas {
		name, err := f.etcd.GetNameByID(id)
		if err != nil {
			return nil, err
		}
		address, err := f.etcd.GetAddress(name, apis.CHUNKSERVER)
		if err != nil {
			return nil, err
		}
		addresses[i] = address
	}
	return addresses, nil
}

func (f *frontend) subscribeReplicas(entry apis.MetadataEntry) ([]apis.Chunkserver, error) {
	replicaAddresses, err := f.getReplicaAddresses(entry)
	if err != nil {
		return nil, err
	}
	replicas := make([]apis.Chunkserver, len(replicaAddresses))
	for i, addr := range replicaAddresses {
		cs, err := f.cache.SubscribeChunkserver(addr)
		if err != nil {
			return nil, err
		}
		replicas[i] = cs
	}
	return replicas, nil
}

// Reads the metadata entry of a particular chunk.
func (f *frontend) ReadMetadataEntry(chunk apis.ChunkNum) (apis.Version, []apis.ServerAddress, error) {
	mc, err := f.getMetadataCache()
	if err != nil {
		return 0, nil, err
	}
	entry, err := mc.ReadEntry(chunk)
	if err != nil {
		return 0, nil, err
	}
	if entry.MostRecentVersion == 0 && entry.LastConsumedVersion > 0 {
		// then this chunk must be in the process of being deleted... don't let them change it!
		return 0, nil, errors.New("chunk is gone: being deleted right now")
	}
	addresses, err := f.getReplicaAddresses(entry)
	if err != nil {
		return 0, nil, err
	}
	return entry.MostRecentVersion, addresses, nil
}

// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
// Only performs the write if the version matches.
func (f *frontend) CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error) {
	cache, err := f.getMetadataCache()
	if err != nil {
		return 0, err
	}
	entry, err := cache.ReadEntry(chunk)
	if err != nil {
		return 0, fmt.Errorf("while fetching metadata entry: %v", err)
	}
	// Confirm that the write can take place to the current version
	if entry.MostRecentVersion != version && version != apis.AnyVersion {
		return entry.MostRecentVersion, fmt.Errorf("incorrect chunk version: write=%d, existing=%d", version, entry.MostRecentVersion)
	}
	if entry.MostRecentVersion == 0 && entry.LastConsumedVersion > 0 {
		// then this chunk must be in the process of being deleted... don't let them change it!
		return 0, errors.New("attempt to write to chunk in the process of deletion")
	}
	// Connect to all of the replicas
	replicas, err := f.subscribeReplicas(entry)
	if err != nil {
		return 0, err
	}
	// Reserve a version for this write
	oldEntry := entry
	if entry.LastConsumedVersion < entry.MostRecentVersion {
		entry.LastConsumedVersion = entry.MostRecentVersion
	}
	entry.LastConsumedVersion += 1
	if err := cache.UpdateEntry(chunk, oldEntry, entry); err != nil {
		return 0, fmt.Errorf("while updating metadata entry: %v", err)
	}
	// Commit the write to the chunkservers
	for _, replica := range replicas {
		if err := replica.CommitWrite(chunk, hash, entry.MostRecentVersion, entry.LastConsumedVersion); err != nil {
			return 0, fmt.Errorf("while commiting writes: %v", err)
		}
	}
	// Update the latest stored metadata version
	oldEntry = entry
	entry.MostRecentVersion = entry.LastConsumedVersion
	if err := cache.UpdateEntry(chunk, oldEntry, entry); err != nil {
		return 0, fmt.Errorf("while updating metadata entry: %v", err)
	}
	// TODO: how to repair if a failure occurs right here
	// Tell the chunkservers to start serving this new version
	for _, replica := range replicas {
		if err := replica.UpdateLatestVersion(chunk, oldEntry.MostRecentVersion, oldEntry.LastConsumedVersion); err != nil {
			return 0, err
		}
	}
	return entry.MostRecentVersion, nil
}

// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
// chunkservers.
func (f *frontend) Delete(chunk apis.ChunkNum, version apis.Version) error {
	cache, err := f.getMetadataCache()
	if err != nil {
		return err
	}
	entry, err := cache.ReadEntry(chunk)
	if err != nil {
		return fmt.Errorf("while fetching metadata entry: %v", err)
	}
	// First, we mark this as deleted
	oldEntry := entry
	entry.MostRecentVersion = 0
	entry.LastConsumedVersion += 1 // just in case it was still zero; this ensures that this is treated as "deleted" and not just "empty"
	if err := cache.UpdateEntry(chunk, oldEntry, entry); err != nil {
		return fmt.Errorf("while updating metadata entry: %v", err)
	}
	// Next, we destroy all of the replica data
	replicas, err := f.subscribeReplicas(entry)
	if err != nil {
		return err
	}
	// TODO: think through all of the failure cases if a service simultaneously deletes the same chunk
	for _, replica := range replicas {
		// TODO: optimize this to not need to list all chunks
		chunks, err := replica.ListAllChunks()
		if err != nil {
			return err
		}
		var firstDeleteError error
		for _, cv := range chunks {
			if cv.Chunk == chunk {
				err := replica.Delete(chunk, cv.Version)
				if err != nil && firstDeleteError == nil {
					// ignore the immediate errors from these, just in case they're caused by a service doing the
					// deletion; instead, we'll check later to make sure everything's gone.
					firstDeleteError = err
				}
			}
		}
		// instead of checking each delete, we just check to make sure everything's gone now
		chunks, err = replica.ListAllChunks()
		if err != nil {
			return err
		}
		for _, cv := range chunks {
			if cv.Chunk == chunk {
				// oh no! these should all be GONE!
				if firstDeleteError != nil {
					return firstDeleteError
				} else {
					return errors.New("unexpectedly not-deleted chunks")
				}
			}
		}
	}
	// Now that all of the replica data is gone, we can get rid of the metadata
	return f.metadata.DeleteEntry(chunk)
}
