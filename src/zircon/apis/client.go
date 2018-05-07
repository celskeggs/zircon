package apis

// A client interface to the Zircon chunk store. This interface is linearizable.
type Client interface {
	// Allocate a new chunk, all zeroed out. The first write must be done with version=0.
	// The chunk is not considered to exist until that first write is performed.
	// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
	New() (ChunkNum, error)

	// Read part or all of the contents of a chunk. offset + length cannot exceed MaxChunkSize.
	// Returns the data read and the version of the data read. The version can be used with Write.
	// If the chunk does not exist, returns an error.
	Read(ref ChunkNum, offset uint32, length uint32) ([]byte, Version, error)

	// Write part or all of the contents of a chunk. offset + len(data) cannot exceed MaxChunkSize.
	// Takes a version; if the version is not AnyVersion and doesn't match the latest version of the chunk, the write is
	// rejected.
	// Returns the new version, if the request succeeds, or the most recent version number, if the request fails due to
	// staleness.
	// If the chunk does not exist, returns an error. If this fails for any reason, there must be no visible change to
	// the underlying data. If this fails for a reason besides staleness, the version must be zero.
	Write(ref ChunkNum, offset uint32, version Version, data []byte) (Version, error)

	// Destroy a chunk, given a specific version number. Version checking works the same as for Write.
	// If the chunk does not exist, returns an error.
	Delete(ref ChunkNum, version Version) error

	// Close all connections used by this client.
	Close() error
}
