package apis

type MetadataID uint64

type Metadata struct {
	MetaID    MetadataID
	Version   Version
	Locations []ServerName
}

type MetadataEntry struct {
	Version  Version
	Replicas []ServerID
}

type MetadataCache interface {
	// TODO
}
