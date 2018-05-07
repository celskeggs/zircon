package access

import (
	"zircon/apis"
	"zircon/rpc"
	"zircon/chunkupdate"
	"fmt"
	"sync"
	"github.com/pkg/errors"
)

type etcdMetadataUpdater struct {
	etcd        apis.EtcdInterface
}

var _ chunkupdate.UpdaterMetadata = &etcdMetadataUpdater{}

func (r *etcdMetadataUpdater) NewEntry() (apis.ChunkNum, error) {
	for i := apis.MinMetadataRange; i <= apis.MaxMetadataRange; i++ {
		data, err := r.etcd.GetMetametadata(i)
		if err != nil {
			return 0, err
		}
		if len(data.Replicas) == 0 && data.LastConsumedVersion == 0 && data.MostRecentVersion == 0 {
			return apis.ChunkNum(i), nil
		}
	}
	return 0, errors.New("no metadata blocks left to allocate!")
}

func (r *etcdMetadataUpdater) ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, error) {
	if apis.MetadataID(chunk) < apis.MinMetadataRange || apis.MetadataID(chunk) > apis.MaxMetadataRange {
		return apis.MetadataEntry{}, errors.New("metadata chunk number not in metadata range")
	}
	return r.etcd.GetMetametadata(apis.MetadataID(chunk))
}

func (r *etcdMetadataUpdater) UpdateEntry(chunk apis.ChunkNum, previous apis.MetadataEntry, next apis.MetadataEntry) error {
	if apis.MetadataID(chunk) < apis.MinMetadataRange || apis.MetadataID(chunk) > apis.MaxMetadataRange {
		return errors.New("metadata chunk number not in metadata range")
	}
	return r.etcd.UpdateMetametadata(apis.MetadataID(chunk), previous, next)
}

func (r *etcdMetadataUpdater) DeleteEntry(chunk apis.ChunkNum, previous apis.MetadataEntry) error {
	return errors.New("cannot delete metametadata")
}
