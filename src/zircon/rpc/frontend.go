package rpc

import (
	"net/http"
	"zircon/apis"
	"zircon/rpc/twirp"
	"context"
)

// Connects to an RPC handler for a Frontend on a certain address.
func UncachedSubscribeFrontend(address apis.ServerAddress, client *http.Client) (apis.Frontend, error) {
	saddr := "http://" + string(address)
	tserve := twirp.NewFrontendProtobufClient(saddr, client)

	return &proxyTwirpAsFrontend{server: tserve}, nil
}

// Starts serving an RPC handler for a Frontend on a certain address. Runs forever.
func PublishFrontend(server apis.Frontend, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	tserve := twirp.NewFrontendServer(&proxyFrontendAsTwirp{server: server}, nil)
	return LaunchEmbeddedHTTP(tserve, address)
}

type proxyFrontendAsTwirp struct {
	server apis.Frontend
}

func (p *proxyFrontendAsTwirp) ReadMetadataEntry(ctx context.Context, request *twirp.Frontend_ReadMetadataEntry) (*twirp.Frontend_ReadMetadataEntry_Result, error) {
	ver, address, err := p.server.ReadMetadataEntry(apis.ChunkNum(request.Chunk))
	if err != nil {
		return nil, err
	}
	return &twirp.Frontend_ReadMetadataEntry_Result{
		Version: uint64(ver),
		Address: AddressArrayToStringArray(address),
	}, nil
}

func (p *proxyFrontendAsTwirp) CommitWrite(ctx context.Context, request *twirp.Frontend_CommitWrite) (*twirp.Frontend_CommitWrite_Result, error) {
	ver, err := p.server.CommitWrite(apis.ChunkNum(request.Chunk), apis.Version(request.Version), apis.CommitHash(request.Hash))
	if err != nil {
		return nil, err
	}
	return &twirp.Frontend_CommitWrite_Result{
		Version: uint64(ver),
	}, nil
}

func (p *proxyFrontendAsTwirp) New(ctx context.Context, request *twirp.Frontend_New) (*twirp.Frontend_New_Result, error) {
	chunk, err := p.server.New()
	if err != nil {
		return nil, err
	}
	return &twirp.Frontend_New_Result{
		Chunk: uint64(chunk),
	}, nil
}

func (p *proxyFrontendAsTwirp) Delete(ctx context.Context, request *twirp.Frontend_Delete) (*twirp.Frontend_Delete_Result, error) {
	err := p.server.Delete(apis.ChunkNum(request.Chunk), apis.Version(request.Version))
	return &twirp.Frontend_Delete_Result{}, err
}

type proxyTwirpAsFrontend struct {
	server twirp.Frontend
}

func (p *proxyTwirpAsFrontend) ReadMetadataEntry(chunk apis.ChunkNum) (apis.Version, []apis.ServerAddress, error) {
	result, err := p.server.ReadMetadataEntry(context.Background(), &twirp.Frontend_ReadMetadataEntry{
		Chunk: uint64(chunk),
	})
	if err != nil {
		return 0, nil, err
	}
	return apis.Version(result.Version), StringArrayToAddressArray(result.Address), nil
}

func (p *proxyTwirpAsFrontend) CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error) {
	result, err := p.server.CommitWrite(context.Background(), &twirp.Frontend_CommitWrite{
		Chunk: uint64(chunk),
		Version: uint64(version),
		Hash: string(hash),
	})
	if err != nil {
		return 0, err
	}
	return apis.Version(result.Version), nil
}

func (p *proxyTwirpAsFrontend) New() (apis.ChunkNum, error) {
	result, err := p.server.New(context.Background(), &twirp.Frontend_New{})
	if err != nil {
		return 0, err
	}
	return apis.ChunkNum(result.Chunk), nil
}

func (p *proxyTwirpAsFrontend) Delete(chunk apis.ChunkNum, version apis.Version) error {
	_, err := p.server.Delete(context.Background(), &twirp.Frontend_Delete{
		Chunk: uint64(chunk),
		Version: uint64(version),
	})
	return err
}
