package chunkupdate
// This package is here to abstract away the details of performing chunk accesses.

import (
	"zircon/apis"
	"sync"
	"fmt"
	"errors"
	"math/rand"
	"zircon/rpc"
)

type Reference struct {
	Chunk    apis.ChunkNum
	Version  apis.Version
	Replicas []apis.ServerAddress
}

type Updater interface {
	New(replicas int) (apis.ChunkNum, error)
	ReadMeta(chunk apis.ChunkNum) (*Reference, error)
	CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error)
	Delete(chunk apis.ChunkNum, version apis.Version) error
}

// Performs a read.
// Preconditions:
//   offset + length <= apis.MaxChunkSize
//   ref is fully populated
// Postconditions:
//   Either returns data and its valid version (of at least this ref's version) read from a chunkserver
//   Or fails, if all chunkservers failed to respond
func (ref *Reference) PerformRead(cache rpc.ConnectionCache, offset uint32, length uint32) ([]byte, apis.Version, error) {
	if offset + length > apis.MaxChunkSize {
		return nil, 0, errors.New("read too long")
	}
	if len(ref.Replicas) == 0 {
		return nil, 0, errors.New("cannot perform read; there are no replicas")
	}
	var lastInnerErr error
	var lastOuterErr error
	// We use rand.Perm so that we'll try the replicas in a random order
	for _, ii := range rand.Perm(len(ref.Replicas)) {
		cs, err := cache.SubscribeChunkserver(ref.Replicas[ii])
		if err == nil {
			data, realVersion, err := cs.Read(ref.Chunk, offset, length, ref.Version)
			if err == nil {
				if uint32(len(data)) != length {
					panic("postcondition on chunkserver.Read(...) violated")
				}
				return data, realVersion, nil
			} else {
				lastInnerErr = err
			}
		} else {
			lastOuterErr = err
		}
	}
	// at this point, we were unsuccessful, and did not manage to read anything
	if lastInnerErr != nil {
		return nil, 0, lastInnerErr
	} else if lastOuterErr != nil {
		return nil, 0, lastOuterErr
	} else {
		panic("should have had an error if we failed")
	}
}

// Prepares a write.
// Preconditions:
//   offset + length <= apis.MaxChunkSize
//   ref is populated
// Postconditions:
//   If possible, all chunkservers have a copy of the data, directly or indirectly.
//   On success, Returns the valid commit hash for this data.
//   Fails if any server fails to connect, directly or indirectly.
func (ref *Reference) PrepareWrite(cache rpc.ConnectionCache, offset uint32, data []byte) (apis.CommitHash, error) {
	if offset + uint32(len(data)) > apis.MaxChunkSize {
		return "", errors.New("write too long")
	}
	if len(ref.Replicas) == 0 {
		return "", errors.New("cannot perform write; there are no replicas")
	}
	addresses := make([]apis.ServerAddress, len(ref.Replicas))
	for i, ii := range rand.Perm(len(ref.Replicas)) {
		addresses[i] = ref.Replicas[ii]
	}
	initial, err := cache.SubscribeChunkserver(addresses[0])
	if err != nil {
		return "", fmt.Errorf("[update.go/CSC] %v", err)
	}
	err = initial.StartWriteReplicated(ref.Chunk, offset, data, addresses[1:])
	if err != nil {
		return "", fmt.Errorf("[update.go/SWR] %v", err)
	}
	return apis.CalculateCommitHash(offset, data), nil
}

type UpdaterMetadata interface {
	NewEntry() (apis.ChunkNum, error)
	ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, error)
	UpdateEntry(chunk apis.ChunkNum, previous apis.MetadataEntry, next apis.MetadataEntry) error
	DeleteEntry(chunk apis.ChunkNum, previous apis.MetadataEntry) error
}

type updater struct {
	mu       sync.Mutex
	cache    rpc.ConnectionCache
	metadata UpdaterMetadata
	etcd     apis.EtcdInterface
}

func NewUpdater(cache rpc.ConnectionCache, etcd apis.EtcdInterface, metadata UpdaterMetadata) Updater {
	return &updater{
		metadata: metadata,
		cache: cache,
		etcd: etcd,
	}
}

func (f *updater) selectInitialChunkservers(replicas int) ([]apis.ServerID, error) {
	if replicas <= 0 {
		return nil, errors.New("must request at least one replica")
	}
	chunkservers, err := ListChunkservers(f.etcd)
	if err != nil {
		return nil, err
	}
	if len(chunkservers) < replicas {
		// TODO: make sure that old chunkservers are autoremoved
		return nil, fmt.Errorf("cannot create new chunks: not enough chunkservers: %v", chunkservers)
	}
	result := make([]apis.ServerID, replicas)
	for i, ii := range rand.Perm(len(chunkservers))[:replicas] {
		result[i] = chunkservers[ii]
	}
	return result, nil
}

// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
// with a version of AnyVersion.
// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
func (f *updater) New(replicaNum int) (apis.ChunkNum, error) {
	// TODO: try to load-balance when initially selecting chunkservers
	replicas, err := f.selectInitialChunkservers(replicaNum)
	if err != nil {
		return 0, fmt.Errorf("[update.go/SIC] %v", err)
	}
	// TODO: garbage collection should look for Version=0 metadata entries and delete them
	chunk, err := f.metadata.NewEntry()
	if err != nil {
		return 0, fmt.Errorf("[update.go/NET] %v", err)
	}
	err = f.metadata.UpdateEntry(chunk, apis.MetadataEntry{}, apis.MetadataEntry{
		MostRecentVersion:   0,
		LastConsumedVersion: 0,
		Replicas:            replicas,
	})
	// TODO: how does garbage collection know not to delete this until the client disconnects early or this server crashes?
	if err != nil {
		// oh well, it'll get cleaned up by garbage collection
		return 0, fmt.Errorf("[update.go/MUE] %v", err)
	}
	// now that we've established the replicas for this chunk, we need to go and tell the chunkservers to store this data
	for _, replica := range replicas {
		address, err := AddressForChunkserver(f.etcd, replica)
		if err != nil {
			return 0, fmt.Errorf("[update.go/AFC] %v", err)
		}
		cs, err := f.cache.SubscribeChunkserver(address)
		if err != nil {
			return 0, fmt.Errorf("[update.go/CSC] %v", err)
		}
		err = cs.Add(chunk, []byte{}, 0)
		if err != nil {
			return 0, fmt.Errorf("[update.go/CSA] %v", err)
		}
	}
	return chunk, nil
}

func (f *updater) getReplicaAddresses(entry apis.MetadataEntry) ([]apis.ServerAddress, error) {
	addresses := make([]apis.ServerAddress, len(entry.Replicas))
	for i, id := range entry.Replicas {
		address, err := AddressForChunkserver(f.etcd, id)
		if err != nil {
			return nil, err
		}
		addresses[i] = address
	}
	return addresses, nil
}

func (f *updater) subscribeReplicas(entry apis.MetadataEntry) ([]apis.Chunkserver, error) {
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
// Preconditions:
//   the chunk exists
//   the chunk is not currently being deleted (i.e. not the case that MRV > LCV)
// Postconditions:
//   the MRV (not the LCV) is returned as the version
//   the chunk is returned as the chunk
//   the list of replicas from the metadata entry is returned in full
func (f *updater) ReadMeta(chunk apis.ChunkNum) (*Reference, error) {
	entry, err := f.metadata.ReadEntry(chunk)
	if err != nil {
		return nil, fmt.Errorf("failure while reading metadata entry: %v", err)
	}
	if entry.MostRecentVersion > entry.LastConsumedVersion {
		// then this chunk must be in the process of being deleted... don't let them read it!
		return nil, errors.New("chunk is gone: being deleted right now")
	}
	addresses, err := f.getReplicaAddresses(entry)
	if err != nil {
		return nil, fmt.Errorf("failure while getting metadata addresses: %v", err)
	}
	return &Reference{
		Chunk: chunk,
		Version: entry.MostRecentVersion,
		Replicas: addresses,
	}, nil
}

// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
// Only performs the write if the version matches.
func (f *updater) CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error) {
	entry, err := f.metadata.ReadEntry(chunk)
	if err != nil {
		return 0, fmt.Errorf("while fetching metadata entry: %v", err)
	}
	if len(entry.Replicas) == 0 {
		return 0, fmt.Errorf("no replicas available for chunk")
	}
	if entry.MostRecentVersion > entry.LastConsumedVersion {
		// then this chunk must be in the process of being deleted... don't let them change it!
		return 0, errors.New("attempt to write to chunk in the process of deletion")
	}
	// Confirm that the write can take place to the current version
	if entry.MostRecentVersion != version && version != apis.AnyVersion {
		return entry.MostRecentVersion, fmt.Errorf("incorrect chunk version: write=%d, existing=%d", version, entry.MostRecentVersion)
	}
	// Connect to all of the replicas
	replicas, err := f.subscribeReplicas(entry)
	if err != nil {
		return 0, err
	}
	// Reserve a version for this write
	oldEntry := entry
	entry.LastConsumedVersion += 1
	if err := f.metadata.UpdateEntry(chunk, oldEntry, entry); err != nil {
		return 0, fmt.Errorf("while updating metadata entry: %v", err)
	}
	// Commit the write to the chunkservers
	for _, replica := range replicas {
		// TODO: accept imperfect durability for the sake of availability
		if err := replica.CommitWrite(chunk, hash, entry.MostRecentVersion, entry.LastConsumedVersion); err != nil {
			return 0, fmt.Errorf("while commiting writes: %v", err)
		}
	}
	// Update the latest stored metadata version
	oldEntry = entry
	entry.MostRecentVersion = entry.LastConsumedVersion
	if err := f.metadata.UpdateEntry(chunk, oldEntry, entry); err != nil {
		return 0, fmt.Errorf("while updating metadata entry: %v", err)
	}
	// TODO: how to repair if a failure occurs right here
	// Tell the chunkservers to start serving this new version
	for _, replica := range replicas {
		// TODO: accept these failures in some way
		if err := replica.UpdateLatestVersion(chunk, oldEntry.MostRecentVersion, oldEntry.LastConsumedVersion); err != nil {
			return 0, err
		}
	}
	return entry.MostRecentVersion, nil
}

// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
// chunkservers.
func (f *updater) Delete(chunk apis.ChunkNum, version apis.Version) error {
	entry, err := f.metadata.ReadEntry(chunk)
	if err != nil {
		return fmt.Errorf("while fetching pre-deletion metadata entry: %v", err)
	}
	if entry.MostRecentVersion > entry.LastConsumedVersion {
		// then this chunk must be in the process of being deleted... don't let them delete it again!
		return errors.New("attempt to delete chunk in the process of deletion")
	}
	if entry.MostRecentVersion != version {
		return errors.New("version mismatch during delete; will not delete")
	}
	// First, we mark this as deleted
	oldEntry := entry
	entry.MostRecentVersion = 0xFFFFFFFFFFFFFFFF
	entry.LastConsumedVersion = 0
	if err := f.metadata.UpdateEntry(chunk, oldEntry, entry); err != nil {
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
					return errors.New("unexpectedly not-deleted chunkservers")
				}
			}
		}
	}
	// Now that all of the replica data is gone, we can get rid of the metadata
	err = f.metadata.DeleteEntry(chunk, entry)
	if err != nil {
		return err
	}
	return nil
}
