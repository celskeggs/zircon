package apis

// A client interface to the Zircon chunk store.
type Client interface {
	// Allocate a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
	// with a version of AnyVersion.
	// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
	New() (ChunkNum, error)

	// Read part or all of the contents of a chunk. offset + length cannot exceed MaxChunkSize.
	// Returns the data read and the version of the data read. The version can be used with Write.
	Read(ref ChunkNum, offset Offset, length Length) ([]byte, Version, error)

	// Write part or all of the contents of a chunk. offset + len(data) cannot exceed MaxChunkSize.
	// Takes a version; if the version is not AnyVersion and doesn't match the latest version of the chunk, the write is
	// rejected.
	// Returns the new version, if the request succeeds, or the most recent version number, if the request fails due to
	// staleness.
	Write(ref ChunkNum, offset Offset, version Version, data []byte) (Version, error)

	// Destroy a chunk, given a specific version number. Version checking works the same as for Write.
	Delete(ref ChunkNum, version Version) error

	// Close all connections used by this client.
	Close() error
}
