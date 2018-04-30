package storage

import "errors"

// Given a path to a raw block device, construct an interface by which a chunkserver can store chunks
func ConfigureBlockStorage(devicepath string) (ChunkStorage, error) {
	return nil, errors.New("block storage not yet implemented")
}
