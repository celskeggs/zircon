package syncserver

import (
	"sync"
	"zircon/apis"
	"sync/atomic"
)

type roundrobin struct {
	servers []apis.SyncServer

	cachedRoot uint64

	mu     sync.Mutex
	nextID int
}

// Constructs an interface to a set of SyncServers as if they were one SyncServer.
// Sends each request to another server. TODO: this is probably a bad way to do it!
// TODO: try the next one on failure
func RoundRobin(servers []apis.SyncServer) apis.SyncServer {
	return &roundrobin{servers: servers}
}

func (r *roundrobin) next() apis.SyncServer {
	r.mu.Lock()
	defer r.mu.Unlock()

	server := r.servers[r.nextID]
	r.nextID = (r.nextID + 1) % len(r.servers)
	return server
}

func (r *roundrobin) StartSync(chunk apis.ChunkNum) (apis.SyncID, error) {
	return r.next().StartSync(chunk)
}

func (r *roundrobin) UpgradeSync(s apis.SyncID) (apis.SyncID, error) {
	return r.next().UpgradeSync(s)
}

func (r *roundrobin) ReleaseSync(s apis.SyncID) error {
	return r.next().ReleaseSync(s)
}

func (r *roundrobin) ConfirmSync(s apis.SyncID) (write bool, err error) {
	return r.next().ConfirmSync(s)
}

// this caches, instead of round-robining
func (r *roundrobin) GetFSRoot() (apis.ChunkNum, error) {
	ichunk := atomic.LoadUint64(&r.cachedRoot)
	if ichunk != 0 { // fastpath
		return apis.ChunkNum(ichunk), nil
	}
	// this entire setup only works because GetFSRoot is guaranteed to always return the same value... so it's fine to duplicate requests.
	chunk, err := r.next().GetFSRoot()
	if err != nil {
		return 0, err
	}
	if chunk == 0 {
		panic("postcondition failed on GetFSRoot")
	}
	atomic.StoreUint64(&r.cachedRoot, uint64(chunk))
	return chunk, nil
}
