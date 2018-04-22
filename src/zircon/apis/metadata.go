package apis

type MetametadataID uint64

type Metametadata struct {
	MetaID MetametadataID
	Version Version
	Locations []ServerName
}

type MetadataCache interface {
	// TODO
}
