package rpc

import (
	"context"
	"net/http"
	"zircon/apis"
	"zircon/rpc/twirp"
)

// Connects to an RPC handler for a SyncServer on a certain address.
func UncachedSubscribeSyncServer(address apis.ServerAddress, client *http.Client) (apis.SyncServer, error) {
	saddr := "http://" + string(address)
	tserve := twirp.NewSyncServerProtobufClient(saddr, client)

	return &proxyTwirpAsSyncServer{server: tserve}, nil
}

// Starts serving an RPC handler for a SyncServer on a certain address. Runs forever.
func PublishSyncServer(server apis.SyncServer, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	tserve := twirp.NewSyncServerServer(&proxySyncServerAsTwirp{server: server}, nil)
	return LaunchEmbeddedHTTP(tserve, address)
}

type proxySyncServerAsTwirp struct {
	server apis.SyncServer
}

func (p *proxySyncServerAsTwirp) StartSync(ctx context.Context, request *twirp.SyncServer_Uint64) (*twirp.SyncServer_Uint64, error) {
	syncid, err := p.server.StartSync(apis.ChunkNum(request.Value))
	if err != nil {
		return nil, err
	}
	return &twirp.SyncServer_Uint64{Value: uint64(syncid)}, nil
}

func (p *proxySyncServerAsTwirp) UpgradeSync(ctx context.Context, request *twirp.SyncServer_Uint64) (*twirp.SyncServer_Uint64, error) {
	syncid, err := p.server.UpgradeSync(apis.SyncID(request.Value))
	if err != nil {
		return nil, err
	}
	return &twirp.SyncServer_Uint64{Value: uint64(syncid)}, nil
}

func (p *proxySyncServerAsTwirp) ReleaseSync(ctx context.Context, request *twirp.SyncServer_Uint64) (*twirp.SyncServer_Nothing, error) {
	err := p.server.ReleaseSync(apis.SyncID(request.Value))
	if err != nil {
		return nil, err
	}
	return &twirp.SyncServer_Nothing{}, nil
}

func (p *proxySyncServerAsTwirp) ConfirmSync(ctx context.Context, request *twirp.SyncServer_Uint64) (*twirp.SyncServer_Bool, error) {
	write, err := p.server.ConfirmSync(apis.SyncID(request.Value))
	if err != nil {
		return nil, err
	}
	return &twirp.SyncServer_Bool{Value: write}, nil
}

func (p *proxySyncServerAsTwirp) GetFSRoot(ctx context.Context, request *twirp.SyncServer_Nothing) (*twirp.SyncServer_Uint64, error) {
	chunk, err := p.server.GetFSRoot()
	if err != nil {
		return nil, err
	}
	return &twirp.SyncServer_Uint64{Value: uint64(chunk)}, nil
}

type proxyTwirpAsSyncServer struct {
	server twirp.SyncServer
}

func (p *proxyTwirpAsSyncServer) StartSync(chunk apis.ChunkNum) (apis.SyncID, error) {
	result, err := p.server.StartSync(context.Background(), &twirp.SyncServer_Uint64{
		Value: uint64(chunk),
	})
	if err != nil {
		return 0, err
	}
	return apis.SyncID(result.Value), nil
}

func (p *proxyTwirpAsSyncServer) UpgradeSync(s apis.SyncID) (apis.SyncID, error) {
	result, err := p.server.UpgradeSync(context.Background(), &twirp.SyncServer_Uint64{
		Value: uint64(s),
	})
	if err != nil {
		return 0, err
	}
	return apis.SyncID(result.Value), nil
}

func (p *proxyTwirpAsSyncServer) ReleaseSync(s apis.SyncID) error {
	_, err := p.server.ReleaseSync(context.Background(), &twirp.SyncServer_Uint64{
		Value: uint64(s),
	})
	return err
}

func (p *proxyTwirpAsSyncServer) ConfirmSync(s apis.SyncID) (write bool, err error) {
	result, err := p.server.ConfirmSync(context.Background(), &twirp.SyncServer_Uint64{
		Value: uint64(s),
	})
	if err != nil {
		return false, err
	}
	return result.Value, nil
}

func (p *proxyTwirpAsSyncServer) GetFSRoot() (apis.ChunkNum, error) {
	result, err := p.server.GetFSRoot(context.Background(), &twirp.SyncServer_Nothing{})
	if err != nil {
		return 0, err
	}
	return apis.ChunkNum(result.Value), nil
}
