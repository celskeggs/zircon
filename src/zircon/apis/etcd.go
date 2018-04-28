package apis

// The name of a server
type ServerName string

// The address of a server
type ServerAddress string

// The space efficient id of a server
type ServerID uint32

type EtcdInterface interface {
	// Get the name of this server
	GetName() ServerName

	// Get the address of a particular server by name
	GetAddress(name ServerName) (ServerAddress, error)
	// Update the address of this server
	UpdateAddress(address ServerAddress) error

	// Attempt to claim a particular metadata block; if already claimed, returns the original owner. if successfully
	// claimed, returns our name.
	TryClaimingMetadata(blockid MetadataID) (owner ServerName, err error)
	// Assuming that this server owns a particular block of metadata, release that metadata back out into the wild.
	DisclaimMetadata(blockid MetadataID) error
	// Get metadata; only allowed if this server has a current claim on the block
	GetMetadata(blockid MetadataID) (Metadata, error)
	// Update metadata; only allowed if this server has a current claim on the block
	UpdateMetadata(blockid MetadataID, data Metadata) error
	// Renew the claim on all metadata blocks
	RenewMetadataClaims() error

	// tear down this connection
	Close() error
}
