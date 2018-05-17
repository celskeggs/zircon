package access

import (
	"zircon/apis"
	"zircon/chunkupdate"
	"errors"
	"fmt"
	"sync"
)

type etcdMetadataUpdater struct {
	etcd             apis.EtcdInterface
	newMutex         sync.Mutex
	localAllocations map[apis.MetadataID]bool
}

var _ chunkupdate.UpdaterMetadata = &etcdMetadataUpdater{}

func (r *etcdMetadataUpdater) NewEntry() (apis.ChunkNum, error) {
	// lock required, so that we don't double-allocate something
	r.newMutex.Lock()
	defer r.newMutex.Unlock()
	for i := apis.MinMetadataRange; i <= apis.MaxMetadataRange; i++ {
		if r.localAllocations[i] {
			continue
		}
		owner, err := r.etcd.TryClaimingMetadata(i)
		if err != nil {
			return 0, fmt.Errorf("while scanning claims for NewEntry: %v", err)
		}
		if owner != r.etcd.GetName() {
			continue // someone else has this, of course
		}
		data, err := r.etcd.GetMetametadata(i)
		if err != nil {
			return 0, fmt.Errorf("while scanning metametadata for NewEntry: %v", err)
		}
		if len(data.Replicas) == 0 && data.LastConsumedVersion == 0 && data.MostRecentVersion == 0 {
			r.localAllocations[i] = true
			return apis.ChunkNum(i), nil
		}
	}
	return 0, errors.New("no metadata blocks left to allocate")
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
