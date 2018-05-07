package access

import (
	"zircon/apis"
	"zircon/rpc"
	"zircon/chunkupdate"
)

const InitialReplicationFactor = 2

type Access struct {
	etcd    apis.EtcdInterface
	cache   rpc.ConnectionCache
	updater chunkupdate.Updater
}

// Construct an access interface for metadata chunks.
func ConstructAccess(etcd apis.EtcdInterface, cache rpc.ConnectionCache) (*Access, error) {
	updater := chunkupdate.NewUpdater(cache, etcd, &etcdMetadataUpdater{
		etcd: etcd,
	})
	return &Access{
		etcd: etcd,
		cache: cache,
		updater: updater,
	}, nil
}

// Allocates a new metadata chunk, all zeroed out. The version number will be zero, so the only way to access it
// initially is with a version of AnyVersion.
// If this chunk isn't written to before the connection to the server closes, the empty chunk may be deleted. (?)
func (f *Access) New() (apis.MetadataID, error) {
	num, err := f.updater.New(InitialReplicationFactor)
	return apis.MetadataID(num), err
}

// Reads a complete metadata chunk.
func (f *Access) Read(chunk apis.MetadataID) ([]byte, apis.Version, error) {
	ref, err := f.updater.ReadMeta(apis.ChunkNum(chunk))
	if err != nil {
		return nil, 0, err
	}
	return ref.PerformRead(f.cache, 0, apis.MaxChunkSize)
}

// Writes part of a metadata chunk. Only performs the write if the version matches.
func (f *Access) Write(chunk apis.MetadataID, version apis.Version, offset uint32, data []byte) (apis.Version, error) {
	ref, err := f.updater.ReadMeta(apis.ChunkNum(chunk))
	if err != nil {
		return 0, err
	}
	hash, err := ref.PrepareWrite(f.cache, offset, data)
	if err != nil {
		return 0, err
	}
	return f.updater.CommitWrite(apis.ChunkNum(chunk), ref.Version, hash)
}
