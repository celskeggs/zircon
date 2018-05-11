package syncserver

import (
	"zircon/apis"
)

type syncServer struct {
	etcd   apis.EtcdInterface
	client apis.Client
}

func NewSyncServer(etcd apis.EtcdInterface, client apis.Client) apis.SyncServer {
	return syncServer{etcd: etcd, client: client}
}

func (s syncServer) StartSync(chunk apis.ChunkNum) (apis.SyncID, error) {
	return s.etcd.StartSync(chunk)
}

func (s syncServer) UpgradeSync(sy apis.SyncID) (apis.SyncID, error) {
	return s.etcd.UpgradeSync(sy)
}

func (s syncServer) ReleaseSync(sy apis.SyncID) error {
	return s.etcd.ReleaseSync(sy)
}

func (s syncServer) ConfirmSync(sy apis.SyncID) (write bool, err error) {
	return s.etcd.ConfirmSync(sy)
}

func (s syncServer) GetFSRoot() (apis.ChunkNum, error) {
	chunk, err := s.etcd.ReadFSRoot()
	if err != nil {
		return 0, err
	}
	if chunk != 0 {
		return chunk, nil
	}
	chunk, err = s.client.New()
	if err != nil {
		return 0, err
	}
	_, err = s.client.Write(chunk, 0, apis.AnyVersion, nil)
	if err != nil {
		return 0, err
	}
	err = s.etcd.WriteFSRoot(chunk)
	if err != nil {
		chunk, err2 := s.etcd.ReadFSRoot()
		if err2 == nil && chunk != 0 {
			return chunk, nil
		} else {
			return 0, err
		}
	}
	return chunk, nil
}
