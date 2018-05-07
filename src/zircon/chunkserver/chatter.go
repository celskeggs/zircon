package chunkserver

import (
	"errors"
	"zircon/apis"
	"zircon/rpc"
	"zircon/util"
)

type wrapper struct {
	Single apis.ChunkserverSingle
	Cache  rpc.ConnectionCache
}

// Supplement a basic chunkserver interface with the ability to connect to other chunkservers
func WithChatter(server apis.ChunkserverSingle, conncache rpc.ConnectionCache) (apis.Chunkserver, error) {
	return &wrapper{Single: server, Cache: conncache}, nil
}

func (w *wrapper) ListAllChunks() ([]struct {
	Chunk   apis.ChunkNum
	Version apis.Version
}, error) {
	return w.Single.ListAllChunks()
}

func (w *wrapper) Add(chunk apis.ChunkNum, initialData []byte, initialVersion apis.Version) error {
	return w.Single.Add(chunk, initialData, initialVersion)
}

func (w *wrapper) Delete(chunk apis.ChunkNum, version apis.Version) error {
	return w.Single.Delete(chunk, version)
}

func (w *wrapper) Read(chunk apis.ChunkNum, offset uint32, length uint32, minimum apis.Version) ([]byte, apis.Version, error) {
	return w.Single.Read(chunk, offset, length, minimum)
}

func (w *wrapper) StartWrite(chunk apis.ChunkNum, offset uint32, data []byte) error {
	return w.Single.StartWrite(chunk, offset, data)
}

func (w *wrapper) CommitWrite(chunk apis.ChunkNum, hash apis.CommitHash, oldVersion apis.Version, newVersion apis.Version) error {
	return w.Single.CommitWrite(chunk, hash, oldVersion, newVersion)
}

func (w *wrapper) UpdateLatestVersion(chunk apis.ChunkNum, oldVersion apis.Version, newVersion apis.Version) error {
	return w.Single.UpdateLatestVersion(chunk, oldVersion, newVersion)
}

func (w *wrapper) StartWriteReplicated(chunk apis.ChunkNum, offset uint32, data []byte, replicas []apis.ServerAddress) error {
	if err := w.Single.StartWrite(chunk, offset, data); err != nil {
		return err
	}
	for _, replica := range replicas {
		server, err := w.Cache.SubscribeChunkserver(replica)
		if err != nil {
			return err
		}
		err = server.StartWrite(chunk, offset, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *wrapper) Replicate(chunk apis.ChunkNum, serverAddress apis.ServerAddress, required apis.Version) error {
	server, err := w.Cache.SubscribeChunkserver(serverAddress)
	if err != nil {
		return err
	}
	data, version, err := w.Single.Read(chunk, 0, apis.MaxChunkSize, required)
	if err != nil {
		return err
	}
	if version != required {
		return errors.New("attempt to replicate from non-primary version")
	}
	return server.Add(chunk, util.StripTrailingZeroes(data), version)
}
