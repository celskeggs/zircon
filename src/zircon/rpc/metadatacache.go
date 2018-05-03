package rpc

import (
	"zircon/apis"
	"net/http"
	"context"
	"zircon/rpc/twirp"
)

// Connects to an RPC handler for a MetadataCache on a certain address.
func UncachedSubscribeMetadataCache(address apis.ServerAddress, client *http.Client) (apis.MetadataCache, error) {
	saddr := "http://" + string(address)
	tserve := twirp.NewMetadataCacheProtobufClient(saddr, client)

	return &proxyTwirpAsMetadataCache{server: tserve}, nil
}

// Starts serving an RPC handler for a MetadataCache on a certain address. Runs forever.
func PublishMetadataCache(server apis.MetadataCache, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	tserve := twirp.NewMetadataCacheServer(&proxyMetadataCacheAsTwirp{server: server}, nil)
	return LaunchEmbeddedHTTP(tserve, address)
}

type proxyMetadataCacheAsTwirp struct {
	server apis.MetadataCache
}

func (p *proxyMetadataCacheAsTwirp) NewEntry(ctx context.Context, request *twirp.MetadataCache_NewEntry) (*twirp.MetadataCache_NewEntry_Result, error) {
	chunk, err := p.server.NewEntry()
	if err != nil {
		return nil, err
	}
	return &twirp.MetadataCache_NewEntry_Result{
		Chunk: uint64(chunk),
	}, nil
}

func (p *proxyMetadataCacheAsTwirp) ReadEntry(ctx context.Context, request *twirp.MetadataCache_ReadEntry) (*twirp.MetadataCache_ReadEntry_Result, error) {
	entry, err := p.server.ReadEntry(apis.ChunkNum(request.Chunk))
	if err != nil {
		return nil, err
	}
	return &twirp.MetadataCache_ReadEntry_Result{
		Entry: &twirp.MetadataEntry{
			Version: uint64(entry.Version),
			ServerIDs: IDArrayToIntArray(entry.Replicas),
		},
	}, nil
}

func (p *proxyMetadataCacheAsTwirp) UpdateEntry(ctx context.Context, request *twirp.MetadataCache_UpdateEntry) (*twirp.MetadataCache_UpdateEntry_Result, error) {
	err := p.server.UpdateEntry(apis.ChunkNum(request.Chunk), apis.MetadataEntry{
		Version:  apis.Version(request.Entry.Version),
		Replicas: IntArrayToIDArray(request.Entry.ServerIDs),
	})
	return &twirp.MetadataCache_UpdateEntry_Result{}, err
}

func (p *proxyMetadataCacheAsTwirp) DeleteEntry(ctx context.Context, request *twirp.MetadataCache_DeleteEntry) (*twirp.MetadataCache_DeleteEntry_Result, error) {
	err := p.server.DeleteEntry(apis.ChunkNum(request.Chunk))
	return &twirp.MetadataCache_DeleteEntry_Result{}, err
}

type proxyTwirpAsMetadataCache struct {
	server twirp.MetadataCache
}

func (p *proxyTwirpAsMetadataCache) NewEntry() (apis.ChunkNum, error) {
	result, err := p.server.NewEntry(context.Background(), &twirp.MetadataCache_NewEntry{})
	if err != nil {
		return 0, err
	}
	return apis.ChunkNum(result.Chunk), nil
}

func (p *proxyTwirpAsMetadataCache) ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, error) {
	result, err := p.server.ReadEntry(context.Background(), &twirp.MetadataCache_ReadEntry{
		Chunk: uint64(chunk),
	})
	if err != nil {
		return apis.MetadataEntry{}, err
	}
	return apis.MetadataEntry{
		Version: apis.Version(result.Entry.Version),
		Replicas: IntArrayToIDArray(result.Entry.ServerIDs),
	}, nil
}

func (p *proxyTwirpAsMetadataCache) UpdateEntry(chunk apis.ChunkNum, entry apis.MetadataEntry) error {
	_, err := p.server.UpdateEntry(context.Background(), &twirp.MetadataCache_UpdateEntry{
		Chunk: uint64(chunk),
		Entry: &twirp.MetadataEntry{
			Version: uint64(entry.Version),
			ServerIDs: IDArrayToIntArray(entry.Replicas),
		},
	})
	return err
}

func (p *proxyTwirpAsMetadataCache) DeleteEntry(chunk apis.ChunkNum) error {
	_, err := p.server.DeleteEntry(context.Background(), &twirp.MetadataCache_DeleteEntry{
		Chunk: uint64(chunk),
	})
	return err
}
