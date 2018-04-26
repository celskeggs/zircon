package apis

// import "github.com/coreos/etcd/clientv3"

// The name of a server
type ServerName string

// The address of a server
type ServerAddress string

type EtcdInterface interface {
	// Get the name of this server
	GetName() ServerName
	// Keep this etcd connection from this frontend alive.
	// If this fails, the server must immediately cease performing operations!
	KeepAlive() error
	// List all of the alive frontends
	ListFrontends() (map[ServerName]ServerAddress, error)

	// Attempt to claim a particular metametadata block; if already claimed, returns the original owner. if successfully
	// claimed, returns our name.
	TryClaimingMetametadata(blockid MetametadataID) (owner ServerName, err error)
	// Assuming that this server owns a particular block of metametadata, release that metadata back out into the wild.
	DisclaimMetametadata(blockid MetametadataID) error
	// Get metametadata; only allowed if this server has a current claim on the block
	GetMetametadata(blockid MetametadataID) (Metametadata, error)
	// Update metametadata; only allowed if this server has a current claim on the block
	UpdateMetametadata(blockid MetametadataID, data Metametadata) error

	// tear down this connection
	Close()
}
