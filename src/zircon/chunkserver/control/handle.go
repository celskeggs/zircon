package control

import (
	"zircon/chunkserver/storage"
	"zircon/apis"
)

type Teardown func()

// an implementation of apis.ChunkserverSingle
type chunkserver struct {
	// TODO
}
var _ apis.ChunkserverSingle = &chunkserver{}

// This includes most of the chunkserver implementation; which it exports through the ChunkserverSingle interface, based
// on a storage layer and a connection to etcd.
func ExposeChunkserver(storage storage.ChunkStorage, etcd apis.EtcdInterface) (apis.ChunkserverSingle, Teardown, error) {
	panic("unimplemented")
}

// Given a chunk reference, read out part or all of a chunk.
// If 'minimum' is AnyVersion, then whichever version the chunkserver currently has will be returned.
// If the version of the chunk that this chunkserver has is at least the minimum version, it will be returned.
// Otherwise, an error will be returned, along with the most recent available version.
// The sum of offset + length must not be greater than MaxChunkSize. The number of bytes returned is always exactly
// the same number of bytes requested if there is no error.
// The version of the data actually read will be returned.
// Fails if a copy of this chunk isn't located on this chunkserver.
func (cs *chunkserver) Read(chunk apis.ChunkNum, offset apis.Offset, length apis.Length, minimum apis.Version) ([]byte, apis.Version, error) {
	panic("unimplemented")
}

// Given a chunk reference, send data to be used for a write to this chunk.
// This method does not actually perform a write.
// The sum of 'offset' and 'len(data)' must not be greater than MaxChunkSize.
// Fails if a copy of this chunk isn't located on this chunkserver.
func (cs *chunkserver) StartWrite(chunk apis.ChunkNum, offset apis.Offset, data []byte) (error) {
	panic("unimplemented")
}

// Commit a write -- persistently store it as the data for a particular version.
// Takes existing saved data for oldVersion, apply this cached write, and saved it as newVersion.
func (cs *chunkserver) CommitWrite(chunk apis.ChunkNum, hash apis.CommitHash, oldVersion apis.Version, newVersion apis.Version) (error) {
	panic("unimplemented")
}

// Update the version of this chunk that will be returned to clients. (Also allowing this chunkserver to delete
// older versions.)
// If the specified chunk does not exist on this chunkserver, errors.
// If the current version reported to clients is different from the oldVersion, errors.
func (cs *chunkserver) UpdateLatestVersion(chunk apis.ChunkNum, oldVersion apis.Version, newVersion apis.Version) error {
	panic("unimplemented")
}

// ** methods used by internal cluster systems **

// Allocates a new chunk on this chunkserver.
// initialData will be padded with zeroes up to the MaxChunkSize
// initialVersion must be positive
func (cs *chunkserver) Add(chunk apis.ChunkNum, initialData []byte, initialVersion apis.Version) (error) {
	panic("unimplemented")
}

// Deletes a chunk stored on this chunkserver with a specific version.
func (cs *chunkserver) Delete(chunk apis.ChunkNum, version apis.Version) (error) {
	panic("unimplemented")
}

// Requests a list of all chunks currently held by this chunkserver.
// There is no guaranteed order for the returned slice.
func (cs *chunkserver) ListAllChunks() ([]struct{ Chunk apis.ChunkNum; Version apis.Version }, error) {
	panic("unimplemented")
}
