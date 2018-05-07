package apis

type MetadataID uint64

type Metametadata struct {
	MetaID    MetadataID
	Chunk     ChunkNum // The chunk the metadata block is present on
	Version   Version
	Locations []ServerName
}

type MetadataEntry struct {
	// these two versions can be mismatched if a write was aborted.
	MostRecentVersion   Version
	LastConsumedVersion Version
	Replicas            []ServerID
}

// Size of a metadata entry in bytes
const EntrySize = 128

// Number of entries per block in bits
const EntriesPerBlock = 15

// Size of the chunk bitset in bytes
const BitsetSize = 4096

// Returned by MetadataCache functions that could redirect the caller, to say that no, this error cannot be fixed by
// trying again on another server.
const NO_REDIRECT = ""

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
