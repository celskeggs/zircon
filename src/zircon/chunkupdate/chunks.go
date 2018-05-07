package chunkupdate

import (
	"zircon/apis"
)

func ListChunkservers(etcd apis.EtcdInterface) ([]apis.ServerID, error) {
	names, err := etcd.ListServers(apis.CHUNKSERVER)
	if err != nil {
		return nil, err
	}
	ids := make([]apis.ServerID, len(names))
	for i, name := range names {
		id, err := etcd.GetIDByName(name)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func AddressForChunkserver(etcd apis.EtcdInterface, chunkserver apis.ServerID) (apis.ServerAddress, error) {
	name, err := etcd.GetNameByID(chunkserver)
	if err != nil {
		return "", err
	}
	return etcd.GetAddress(name, apis.CHUNKSERVER)
}
