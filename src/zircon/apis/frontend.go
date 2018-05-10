package apis

import (
	"crypto/sha256"
	"fmt"
	"encoding/hex"
)

// A hash of a write at a particular offset with a particular length and data.
type CommitHash string

type Frontend interface {
	// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
	// with a version of AnyVersion.
	// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
	New() (ChunkNum, error)

	// Reads the metadata entry of a particular chunk.
	ReadMetadataEntry(chunk ChunkNum) (Version, []ServerAddress, error)

	// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
	// Only performs the write if the version matches, or the version is AnyVersion.
	CommitWrite(chunk ChunkNum, version Version, hash CommitHash) (Version, error)

	// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
	// chunkservers.
	Delete(chunk ChunkNum, version Version) error
}

// Calculates a hash of a write. This is used to ensure that the same data has been replicated to all chunkservers,
// without having to compare the entire message.
func CalculateCommitHash(offset uint32, data []byte) CommitHash {
	hashInput := fmt.Sprintf("%d %d %s", offset, len(data), string(data))
	hashArray := sha256.Sum256([]byte(hashInput))
	return CommitHash(hex.EncodeToString(hashArray[:]))
}
