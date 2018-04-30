package frontend

import (
	"sync"
	"zircon/apis"
)

type roundrobin struct {
	servers []apis.Frontend

	mu     sync.Mutex
	nextID int
}

// Constructs an interface to a set of frontends as if they were one front-end.
// Sends each request to another server. TODO: this is probably a bad way to do it!
func RoundRobin(servers []apis.Frontend) apis.Frontend {
	return &roundrobin{servers: servers}
}

func (r *roundrobin) next() apis.Frontend {
	r.mu.Lock()
	defer r.mu.Unlock()

	server := r.servers[r.nextID]
	r.nextID = (r.nextID + 1) % len(r.servers)
	return server
}

func (r *roundrobin) ReadMetadata(chunk apis.ChunkNum) (apis.Version, []apis.ServerAddress, error) {
	return r.next().ReadMetadata(chunk)
}

func (r *roundrobin) CommitWrite(chunk apis.ChunkNum, version apis.Version, hash apis.CommitHash) (apis.Version, error) {
	return r.next().CommitWrite(chunk, version, hash)
}

func (r *roundrobin) New() (apis.ChunkNum, error) {
	return r.next().New()
}

func (r *roundrobin) Delete(chunk apis.ChunkNum, version apis.Version) error {
	return r.next().Delete(chunk, version)
}
