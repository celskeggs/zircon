package apis

// Note: the metadata chunk for metadata block N is stored in chunk N
// Note: this means that there is NO METADATA BLOCK for 0! because that would be metametadata, which is stored in etcd.
type MetadataID uint64

const MinMetadataRange MetadataID = 1
const MaxMetadataRange MetadataID = 1024 // TODO: update this so that it's the actual <beginning of metablock 1>-1 offset

type MetadataEntry struct {
	// these two versions can be mismatched if a write was aborted.
	MostRecentVersion   Version
	LastConsumedVersion Version
	Replicas            []ServerID
}

func (me MetadataEntry) Equals(other MetadataEntry) bool {
	if me.MostRecentVersion != other.MostRecentVersion {
		return false
	}
	if me.LastConsumedVersion != other.LastConsumedVersion {
		return false
	}
	if len(me.Replicas) != len(other.Replicas) {
		return false
	}
	for i, myReplica := range me.Replicas {
		if other.Replicas[i] != myReplica {
			return false
		}
	}
	return true
}

// Size of a metadata entry in bytes
const EntrySize = 128

// Number of entries per block in bits
const EntriesPerBlock = 15

// Size of the chunk bitset in bytes
const BitsetSize = 4096

// Returned by MetadataCache functions that could redirect the caller, to say that no, this error cannot be fixed by
// trying again on another server.
const NoRedirect = ""

type MetadataCache interface {
	// Allocate a new metadata entry and corresponding chunk number
	NewEntry() (ChunkNum, error)
	// Reads the metadata entry of a particular chunk.
	// If another server holds the lease on the metametadata the entry belongs to, returns it's name
	ReadEntry(chunk ChunkNum) (MetadataEntry, ServerName, error)
	// Update the metadate entry of a particular chunk.
	// If another server holds the lease on the metametadata the entry belongs to, returns it's name
	UpdateEntry(chunk ChunkNum, previousEntry MetadataEntry, newEntry MetadataEntry) (ServerName, error)
	// Delete a metadata entry and allow the garbage collection of the underlying chunks
	// If another server holds the lease on the metametadata the entry belongs to, returns it's name
	DeleteEntry(chunk ChunkNum, previousEntry MetadataEntry) (ServerName, error)
}
