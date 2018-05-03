package apis

type MetadataID uint64

type Metametadata struct {
	MetaID    MetadataID
	Version   Version
	Locations []ServerName
}

type MetadataEntry struct {
	// these two versions can be mismatched if a write was aborted.
	MostRecentVersion   Version
	LastConsumedVersion Version
	Replicas            []ServerID
}

type MetadataCache interface {
	// Allocate a new metadata entry and corresponding chunk number
	NewEntry() (ChunkNum, error)
	// Reads the metadata entry of a particular chunk.
	ReadEntry(chunk ChunkNum) (MetadataEntry, error)
	// Update the metadate entry of a particular chunk.
	UpdateEntry(chunk ChunkNum, previousEntry MetadataEntry, newEntry MetadataEntry) error
	// Delete a metadata entry and allow the garbage collection of the underlying chunks
	DeleteEntry(chunk ChunkNum) error
}
