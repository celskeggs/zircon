package apis

import (
	"crypto/sha256"
	"fmt"
)

// A hash of a write at a particular offset with a particular length and data.
type CommitHash string

type Frontend interface {
	// Reads metadata about a particular chunk.
	ReadMetadata(chunk ChunkNum) (Version, []ServerAddress, error)

	// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
	// Only performs the write if the version matches.
	CommitWrite(chunk ChunkNum, version Version, hash CommitHash) (Version, error)

	// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
	// with a version of AnyVersion.
	// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
	New() (ChunkNum, error)

	// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
	// chunkservers.
	Delete(chunk ChunkNum, version Version)
}

// Calculates a hash of a write. This is used to ensure that the same data has been replicated to all chunkservers,
// without having to compare the entire message.
func CalculateCommitHash(offset Offset, data []byte) CommitHash {
	hashInput := fmt.Sprintf("%d %d %s", offset, len(data), string(data))
	hashArray := sha256.Sum256([]byte(hashInput))
	return CommitHash(hashArray[:])
}
