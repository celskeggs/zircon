package etcd

import (
	"zircon/apis"
	"github.com/coreos/etcd/clientv3"
)

type etcdinterface struct {
	LocalName apis.ServerName
	Client    *clientv3.Client
}

// Connects to etcd and provides our specific etcd interface based on that connection.
func SubscribeEtcd(localName apis.ServerName, servers []apis.ServerAddress) (apis.EtcdInterface, error) {
	endpoints := make([]string, len(servers))
	for i, v := range servers {
		endpoints[i] = string(v)
	}
	client, err := clientv3.NewFromURLs(endpoints)
	if err != nil {
		return nil, err
	}
	return &etcdinterface{
		LocalName: localName,
		Client: client,
	}, nil
}

// Get the name of this server
func (e *etcdinterface) GetName() apis.ServerName {
	return e.LocalName
}

// Get the address of a particular server by name
func (e *etcdinterface) GetAddress(name apis.ServerName) (apis.ServerAddress, error) {
	panic("unimplemented")
}

// Update the address of this server
func (e *etcdinterface) UpdateAddress(address apis.ServerAddress) error {
	panic("unimplemented")
}

// Attempt to claim a particular metadata block; if already claimed, returns the original owner. if successfully
// claimed, returns our name.
func (e *etcdinterface) TryClaimingMetadata(blockid apis.MetadataID) (owner apis.ServerName, err error) {
	panic("unimplemented")
}

// Assuming that this server owns a particular block of metadata, release that metadata back out into the wild.
func (e *etcdinterface) DisclaimMetadata(blockid apis.MetadataID) error {
	panic("unimplemented")
}

// Get metadata; only allowed if this server has a current claim on the block
func (e *etcdinterface) GetMetadata(blockid apis.MetadataID) (apis.Metadata, error) {
	panic("unimplemented")
}

// Update metadata; only allowed if this server has a current claim on the block
func (e *etcdinterface) UpdateMetadata(blockid apis.MetadataID, data apis.Metadata) error {
	panic("unimplemented")
}

// Renew the claim on all metadata blocks
func (e *etcdinterface) RenewMetadataClaims() error {
	panic("unimplemented")
}

// tear down this connection
func (e *etcdinterface) Close() error {
	return e.Client.Close()
}
