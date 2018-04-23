package storage

import "zircon/apis"

type ChunkStorage interface {
	// *** part 1: chunks ***

	// List chunks for which we have *any* stored versions
	ListChunksWithData() ([]apis.ChunkNum, error)

	// List all versions we have for a certain chunk
	ListVersions(chunk apis.ChunkNum) ([]apis.Version, error)
	// Read the entire contents of a particular version of a particular chunk
	ReadVersion(chunk apis.ChunkNum, version apis.Version) ([]byte, error)
	// Write the entire contents of a new version for a chunk.
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
}
