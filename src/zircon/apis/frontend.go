package apis

import (
	"crypto/sha256"
	"fmt"
)

// A hash of a write at a particular offset with a particular length and data.
type CommitHash string
// An ID of an access control entity. If positive, represents a named entity. If negative, represents a defined ACL.
// If zero, means "everyone has access".
type AccessControlID int64

// Means "everyone has access"
const UnrestrictedAccess AccessControlID = 0

type Frontend interface {
	// Reads metadata about a particular chunk.
	ReadMetadata(chunk ChunkNum) (AccessControlID, Version, ChunkReference, []ServerAddress, error)

	// Writes metadata for a particular chunk, after each chunkserver has received a preparation message for this write.
	// Only performs the write if the version matches.
	CommitWrite(chunk ChunkNum, version Version, hash CommitHash) (Version, error)

	// Allocates a new chunk, all zeroed out. The version number will be zero, so the only way to access it initially is
	// with a version of AnyVersion.
	// If this chunk isn't written to before the connection to the server closes, the empty chunk will be deleted.
	New(aci AccessControlID) (ChunkNum, error)

	// Destroys an old chunk, assuming that the metadata version matches. This includes sending messages to all relevant
	// chunkservers.
	Delete(num ChunkNum, version Version)

	// Retrieve the access control ID for the current client.
	GetMyAccessID() AccessControlID

	// Create a new access control list, owned by a certain access control entity, and containing a certain set of
	// entities with access.
	CreateAccessControlList(owner AccessControlID, entities []AccessControlID) (AccessControlID, error)

	// Get the contents of an existing access control list.
	ReadAccessControlList(aci AccessControlID) (owner AccessControlID, entities []AccessControlID, err error)

	// Update an existing access control list.
	UpdateAccessControlList(aci AccessControlID, owner AccessControlID, entities []AccessControlID) error

	// Delete an existing access control list.
	DeleteAccessControlList(aci AccessControlID) error
}

// Calculates a hash of a write. This is used to ensure that the same data has been replicated to all chunkservers,
// without having to compare the entire message.
func CalculateCommitHash(offset Offset, data []byte) CommitHash {
	hashInput := fmt.Sprintf("%d %d %s", offset, len(data), string(data))
	hashArray := sha256.Sum256([]byte(hashInput))
	return CommitHash(hashArray[:])
}
