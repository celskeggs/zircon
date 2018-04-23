package storage

import "errors"

// Given a base path for storage of files in a modern filesystem, construct an interface by which a chunkserver can store
// chunks.
func ConfigureFilesystemStorage(basepath string) (ChunkStorage, error) {
	return nil, errors.New("filesystem storage not yet implemented")
}
