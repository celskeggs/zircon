package control

import (
	"zircon/apis"
	"zircon/chunkupdate"
)

type chunkserverUpdater struct {
	etcd  apis.EtcdInterface
}

var _ chunkupdate.UpdaterChunkservers = &chunkserverUpdater{}

func (c *chunkserverUpdater) List() ([]apis.ServerID, error) {
	names, err := c.etcd.ListServers(apis.CHUNKSERVER)
	if err != nil {
		return nil, err
	}
	ids := make([]apis.ServerID, len(names))
	for i, name := range names {
		id, err := c.etcd.GetIDByName(name)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (c *chunkserverUpdater) Address(chunkserver apis.ServerID) (apis.ServerAddress, error) {
	name, err := c.etcd.GetNameByID(chunkserver)
	if err != nil {
		return "", err
	}
	return c.etcd.GetAddress(name, apis.CHUNKSERVER)
}
