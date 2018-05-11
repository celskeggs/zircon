package apis

import "time"

// The name of a server
type ServerName string

// The address of a server
type ServerAddress string

// The space efficient id of a server
type ServerID uint32

// A type of server
type ServerType int

const (
	FRONTEND      ServerType = iota
	METADATACACHE ServerType = iota
	CHUNKSERVER   ServerType = iota
)

type EtcdInterface interface {
	// Get the name of this server
	GetName() ServerName

	// Get the address of a particular server by name, or returns an error if it doesn't exist.
	GetAddress(name ServerName, kind ServerType) (ServerAddress, error)
	// Update the address and type of this server. Assigns an ID if necessary.
	UpdateAddress(address ServerAddress, kind ServerType) error
	// Get the name corresponding to a ServerID
	GetNameByID(id ServerID) (ServerName, error)
	// Get the ServerID corresponding to a name
	GetIDByName(name ServerName) (ServerID, error)
	// Lists server names by type of server
	ListServers(kind ServerType) ([]ServerName, error)

	// Prepares this interface to accept claims for metadata
	BeginMetadataLease() error
	// Gets the metadata lease timeout for this configuration.
	GetMetadataLeaseTimeout() time.Duration
	// Attempt to claim a particular metadata block; if already claimed, returns the original owner (no error).
	// if successfully claimed, returns our name.
	TryClaimingMetadata(blockid MetadataID) (owner ServerName, err error)
	// Assuming that this server owns a particular block of metadata, release that metadata back out into the wild.
	DisclaimMetadata(blockid MetadataID) error
	// Claim some unclaimed metametablock. If everything that exists is claimed, return 0 and no error.
	LeaseAnyMetametadata() (MetadataID, error)
	// Lists the MetadataIDs of every metadata block that exists
	ListAllMetaIDs() ([]MetadataID, error)
	// Renew the claim on all metadata blocks
	RenewMetadataClaims() error

	// Get metametadata for a metadata block; only allowed if this server has a current claim on the block
	GetMetametadata(blockid MetadataID) (MetadataEntry, error)
	// Update metametadata for a metadata block; only allowed if this server has a current claim on the block
	// If the previous value does not match the current contents, fails.
	UpdateMetametadata(blockid MetadataID, previous MetadataEntry, data MetadataEntry) error

	// The syncserver is just a direct etcd interface; incorporate it like this.
	SyncServerDirect

	// Writes the filesystem root chunk number
	WriteFSRoot(chunk ChunkNum) (error)

	// Reads the filesystem root chunk number, or 0 if nonexistent
	ReadFSRoot() (ChunkNum, error)

	// tear down this connection
	Close() error
}
