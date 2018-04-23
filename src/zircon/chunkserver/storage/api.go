package storage

import "zircon/apis"

// An interface to a storage system for chunks and version information.
// This interface is expected to be write-immediate; changes made should be
// flushed to disk before each mutation returns.
type ChunkStorage interface {
	// *** part 1: chunks ***

	// List chunks for which we have *any* stored versions, in no particular order
	ListChunksWithData() ([]apis.ChunkNum, error)

	// List all versions we have for a certain chunk, in ascending order
	ListVersions(chunk apis.ChunkNum) ([]apis.Version, error)
	// Read the entire contents of a particular version of a particular chunk
	// note: version *cannot* be AnyVersion
	ReadVersion(chunk apis.ChunkNum, version apis.Version) ([]byte, error)
	// Write the entire contents of a new version for a chunk.
	// data cannot be larger than apis.MaxChunkSize. The storage layer may pad
	// out the written data with additional zeroes, up to apis.MaxChunkSize.
	WriteVersion(chunk apis.ChunkNum, version apis.Version, data []byte) error
	// Delete an existing version of a chunk.
	DeleteVersion(chunk apis.ChunkNum, version apis.Version) error


	// *** part 2: versions ***

	// List chunks for which we've stored a latest version.
	ListChunksWithLatest() ([]apis.ChunkNum, error)

	// Get the "latest version" (to report to clients) of a particular chunk.
	GetLatestVersion(chunk apis.ChunkNum) (apis.Version, error)
	// Update the "latest version" (to report to clients) of a particular chunk.
	SetLatestVersion(chunk apis.ChunkNum, latest apis.Version) error
	// Remove records storing the latest version for a particular chunk.
	DeleteLatestVersion(chunk apis.ChunkNum) error

	// Empty any caches and tear down all storage state.
	Close()
}
