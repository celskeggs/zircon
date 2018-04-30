package apis

type MetametadataID uint64

type Metametadata struct {
	MetaID MetametadataID
	Version Version
	Locations []ServerName
}

type Metadata struct {
	Version  Version
	Replicas []ServerID
}

type MetadataCache interface {
	// TODO
}
