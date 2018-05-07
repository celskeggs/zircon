package frontend

import (
	"zircon/apis"
	"zircon/rpc"
	"zircon/chunkupdate"
)

const InitialReplicationFactor = 2

type frontend struct {
	etcd    apis.EtcdInterface
	cache   rpc.ConnectionCache
	updater chunkupdate.Updater
}

// Construct a frontend server, not including metadata caches and service handlers.
func ConstructFrontend(etcd apis.EtcdInterface, cache rpc.ConnectionCache) (apis.Frontend, error) {
	updater := chunkupdate.NewUpdater(cache, etcd, &reselectingMetadataUpdater{
		etcd: etcd,
		cache: cache,
	})
	return &frontend{
		etcd: etcd,
		cache: cache,
		updater: updater,
	}, nil
}

// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
// with a version of AnyVersion.
// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
func (f *frontend) New() (apis.ChunkNum, error) {
	return f.updater.New(InitialReplicationFactor)
}

// Reads the metadata entry of a particular chunk.
func (f *frontend) ReadMetadataEntry(chunk apis.ChunkNum) (apis.Version, []apis.ServerAddress, error) {
	ref, err := f.updater.ReadMeta(chunk)
	return ref.Version, ref.Replicas, err
}

// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
// Only performs the write if the version matches.
func (f *frontend) CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error) {
	return f.updater.CommitWrite(chunk, version, hash)
}

// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
// chunkservers.
func (f *frontend) Delete(chunk apis.ChunkNum, version apis.Version) error {
	return f.updater.Delete(chunk, version)
}
