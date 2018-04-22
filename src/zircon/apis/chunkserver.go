package apis

import "time"

// The version number of a chunk
type Version uint64
// An offset within a chunk
type Offset uint32
// A length of data within a chunk
type Length uint32
// A chunk identifier, not directly exposed to normal clients
type ChunkNum uint64

// 8 MiB, the maximum size of a chunk stored on the chunkserver
const MaxChunkSize Length = 8 * 1024 * 1024

// Represents "any version is valid" when passed as a chunk version number
const AnyVersion Version = 0

// An opaque reference to a particular chunk.
// Does not include which server to look on, but does include the chunk ID.
// This acts as a bearer token -- whoever has a copy of it has permission to perform operations on this chunk.
type ChunkReference struct {
	Chunk       ChunkNum
	IsReadWrite bool
	// TODO: figure this security part out
	Expiration  time.Time
	Signature   []byte
}

// note: this API is strongly consistent, because it's a connection to just a single chunkserver
type Chunkserver interface {
	// ** methods used by clients and metadata caches **

	// Given a chunk reference, read out part or all of a chunk.
	// If 'minimum' is AnyVersion, then whichever version the chunkserver currently has will be returned.
	// If the version of the chunk that this chunkserver has is at least the minimum version, it will be returned.
	// Otherwise, an error will be returned, along with the most recent available version.
	// The sum of offset + length must not be greater than MaxChunkSize. The number of bytes returned is always exactly
	// the same number of bytes requested, unless an error condition is signaled.
	// The version of the data actually read will be returned.
	// Fails if a copy of this chunk isn't located on this chunkserver.
	Read(ref ChunkReference, offset Offset, length Length, minimum Version) ([]byte, Version, error)

	// Given a chunk reference, begin a write to that chunk.
	// The sum of 'offset' and 'len(data)' must not be greater than MaxChunkSize.
	// If replicas is nonempty, this will also replicate the prepared write to those servers.
	// Fails if a copy of this chunk isn't located on this chunkserver, or if another server fails to prepare.
	StartWrite(ref ChunkReference, offset Offset, data []byte, replicas []ServerAddress) (error)
}

type ChunkserverAdmin interface {
	// these are the same as in ChunkServer, except they don't need authentication.
	Read(chunk ChunkNum, offset Offset, length Length, minimum Version) ([]byte, Version, error)
	StartWrite(chunk ChunkNum, offset Offset, data []byte) (Version, error)

	// ** methods used by internal cluster systems **

	// Allocates a new chunk on this chunkserver.
	// initialData will be padded with zeroes up to the MaxChunkSize
	// initialVersion must be positive
	// Fails if this
	Add(chunk ChunkNum, initialData []byte, initialVersion Version) (error)

	// Deletes a chunk stored on this chunkserver with a specific version.
	Delete(chunk ChunkNum) (error)

	// Tells this chunkserver to directly replicate a particular chunk to another specified chunkserver.
	// This will use 'subref' to call 'Add' on the other chunkserver at 'serverAddress'.
	// Replication will only take place assuming that the 'version' specified is the version stored.
	// This will return success once the operation has completed successfully.
	Replicate(serverAddress ServerAddress, subref ChunkReference, version Version) (error)

	// Requests a list of all chunks currently held by this chunkserver.
	ListAllChunks() ([]struct{ Chunk ChunkNum; Version Version }, error)
}
