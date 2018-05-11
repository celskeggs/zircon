package rpc

import (
	"context"
	"errors"
	"net/http"
	"zircon/apis"
	"zircon/rpc/twirp"
)

// Connects to an RPC handler for a Chunkserver on a certain address.
func UncachedSubscribeChunkserver(address apis.ServerAddress, client *http.Client) (apis.Chunkserver, error) {
	saddr := "http://" + string(address)
	tserve := twirp.NewChunkserverProtobufClient(saddr, client)

	return &proxyTwirpAsChunkserver{server: tserve}, nil
}

// Starts serving an RPC handler for a Chunkserver on a certain address. Runs forever.
func PublishChunkserver(server apis.Chunkserver, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	tserve := twirp.NewChunkserverServer(&proxyChunkserverAsTwirp{server: server}, nil)
	return LaunchEmbeddedHTTP(tserve, address)
}

type proxyChunkserverAsTwirp struct {
	server apis.Chunkserver
}

func (p *proxyChunkserverAsTwirp) StartWriteReplicated(context context.Context, input *twirp.Chunkserver_StartWriteReplicated) (*twirp.Nothing, error) {
	err := p.server.StartWriteReplicated(apis.ChunkNum(input.Chunk), input.Offset, input.Data, StringArrayToAddressArray(input.Addresses))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) Replicate(context context.Context, input *twirp.Chunkserver_Replicate) (*twirp.Nothing, error) {
	err := p.server.Replicate(apis.ChunkNum(input.Chunk), apis.ServerAddress(input.ServerAddress), apis.Version(input.Version))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) Read(context context.Context, input *twirp.Chunkserver_Read) (*twirp.Chunkserver_Read_Result, error) {
	data, version, err := p.server.Read(apis.ChunkNum(input.Chunk), input.Offset, input.Length, apis.Version(input.Version))
	message := ""
	if err != nil {
		message = err.Error()
		if message == "" {
			panic("expected nonempty error code")
		}
	}
	return &twirp.Chunkserver_Read_Result{
		Data:    data,
		Version: uint64(version),
		Error:   message,
	}, nil
}

func (p *proxyChunkserverAsTwirp) StartWrite(context context.Context, input *twirp.Chunkserver_StartWrite) (*twirp.Nothing, error) {
	err := p.server.StartWrite(apis.ChunkNum(input.Chunk), input.Offset, input.Data)
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) CommitWrite(context context.Context, input *twirp.Chunkserver_CommitWrite) (*twirp.Nothing, error) {
	err := p.server.CommitWrite(apis.ChunkNum(input.Chunk), apis.CommitHash(input.Hash), apis.Version(input.OldVersion), apis.Version(input.NewVersion))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) UpdateLatestVersion(context context.Context, input *twirp.Chunkserver_UpdateLatestVersion) (*twirp.Nothing, error) {
	err := p.server.UpdateLatestVersion(apis.ChunkNum(input.Chunk), apis.Version(input.OldVersion), apis.Version(input.NewVersion))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) Add(context context.Context, input *twirp.Chunkserver_Add) (*twirp.Nothing, error) {
	err := p.server.Add(apis.ChunkNum(input.Chunk), input.InitialData, apis.Version(input.Version))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) Delete(context context.Context, input *twirp.Chunkserver_Delete) (*twirp.Nothing, error) {
	err := p.server.Delete(apis.ChunkNum(input.Chunk), apis.Version(input.Version))
	return &twirp.Nothing{}, err
}

func (p *proxyChunkserverAsTwirp) ListAllChunks(context.Context,
	*twirp.Nothing) (*twirp.Chunkserver_ListAllChunks_Result, error) {
	chunks, err := p.server.ListAllChunks()

	chunkVersions := make([]*twirp.ChunkVersion, len(chunks))
	for i, chunk := range chunks {
		chunkVersions[i] = &twirp.ChunkVersion{
			Chunk:   uint64(chunk.Chunk),
			Version: uint64(chunk.Version),
		}
	}

	return &twirp.Chunkserver_ListAllChunks_Result{
		Chunks: chunkVersions,
	}, err
}

type proxyTwirpAsChunkserver struct {
	server twirp.Chunkserver
}

func (p *proxyTwirpAsChunkserver) StartWriteReplicated(chunk apis.ChunkNum, offset uint32, data []byte,
	replicas []apis.ServerAddress) error {

	_, err := p.server.StartWriteReplicated(context.Background(), &twirp.Chunkserver_StartWriteReplicated{
		Chunk:     uint64(chunk),
		Offset:    offset,
		Data:      data,
		Addresses: AddressArrayToStringArray(replicas),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) Replicate(chunk apis.ChunkNum, serverAddress apis.ServerAddress,
	version apis.Version) error {
	_, err := p.server.Replicate(context.Background(), &twirp.Chunkserver_Replicate{
		Chunk:         uint64(chunk),
		ServerAddress: string(serverAddress),
		Version:       uint64(version),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) Read(chunk apis.ChunkNum, offset uint32, length uint32, minimum apis.Version) ([]byte, apis.Version, error) {
	result, err := p.server.Read(context.Background(), &twirp.Chunkserver_Read{
		Chunk:   uint64(chunk),
		Offset:  offset,
		Length:  length,
		Version: uint64(minimum),
	})
	if err != nil {
		return nil, 0, err
	}
	if result.Error != "" {
		return nil, apis.Version(result.Version), errors.New(result.Error)
	}
	return result.Data, apis.Version(result.Version), nil
}

func (p *proxyTwirpAsChunkserver) StartWrite(chunk apis.ChunkNum, offset uint32, data []byte) error {
	_, err := p.server.StartWrite(context.Background(), &twirp.Chunkserver_StartWrite{
		Chunk:  uint64(chunk),
		Offset: offset,
		Data:   data,
	})
	return err
}

func (p *proxyTwirpAsChunkserver) CommitWrite(chunk apis.ChunkNum, hash apis.CommitHash, oldVersion apis.Version,
	newVersion apis.Version) error {
	_, err := p.server.CommitWrite(context.Background(), &twirp.Chunkserver_CommitWrite{
		Chunk:      uint64(chunk),
		Hash:       string(hash),
		OldVersion: uint64(oldVersion),
		NewVersion: uint64(newVersion),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) UpdateLatestVersion(chunk apis.ChunkNum, oldVersion apis.Version,
	newVersion apis.Version) error {
	_, err := p.server.UpdateLatestVersion(context.Background(), &twirp.Chunkserver_UpdateLatestVersion{
		Chunk:      uint64(chunk),
		OldVersion: uint64(oldVersion),
		NewVersion: uint64(newVersion),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) Add(chunk apis.ChunkNum, initialData []byte, initialVersion apis.Version) error {
	_, err := p.server.Add(context.Background(), &twirp.Chunkserver_Add{
		Chunk:       uint64(chunk),
		InitialData: initialData,
		Version:     uint64(initialVersion),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) Delete(chunk apis.ChunkNum, version apis.Version) error {
	_, err := p.server.Delete(context.Background(), &twirp.Chunkserver_Delete{
		Chunk:   uint64(chunk),
		Version: uint64(version),
	})
	return err
}

func (p *proxyTwirpAsChunkserver) ListAllChunks() ([]apis.ChunkVersion, error) {
	result, err := p.server.ListAllChunks(context.Background(), &twirp.Nothing{})
	decoded := make([]apis.ChunkVersion, len(result.Chunks))
	for i, v := range result.Chunks {
		decoded[i] = apis.ChunkVersion{
			Chunk:   apis.ChunkNum(v.Chunk),
			Version: apis.Version(v.Version),
		}
	}
	return decoded, err
}
