package services

import (
	"errors"
	"fmt"
	"log"
	"time"
	"zircon/apis"
	"zircon/chunkupdate"
	"zircon/rpc"
)

const MaxUint = ^uint(0)
const MaxInt = int(MaxUint >> 1)

// The chunk server with the most chunks should have at most maxChunkRatio times
// more chunks than the one with the least chunks
const maxChunkRatio = 2

// Replicaiton Frequency in seconds
const BalancingFreq = 5

// TODO This whole thing
func LoadBalancerService(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	cancel = func() error {
		return nil
	}

	return cancel, nil
}

type balancer struct {
	etcd       apis.EtcdInterface
	localCache apis.MetadataCache
	rpcCache   rpc.ConnectionCache
	stop       bool
}

func (bal *balancer) Start() error {
	go func() {
		for !bal.stop {
			bal.balance()

			time.Sleep(BalancingFreq * time.Second)
		}
	}()

	return nil
}

func (bal *balancer) Stop() error {
	bal.stop = true
	return nil
}

// TODO Balancer currently assumes each node to have the same amount of storage,
// ability to handle load, e.g. Poss. change this to allow balancing of unequal nodes
func (bal *balancer) balance() error {
	// Generate a list of valid chunk refences per chunkserver
	validChunks, err := bal.genValidChunks()
	if err != nil {
		return err
	}

	// Find the chunkserver with the most elements and the one with the least
	maxID, max := maxChunkserver(validChunks)
	minID, min := minChunkserver(validChunks)
	for max > 0 && max > min*2 {
		// Transfer a chunk from the maximal chunkserver to the minimal chunkserver
		err := bal.transferSomeChunk(maxID, minID, validChunks)
		if err != nil {
			return err
		}

		maxID, max = maxChunkserver(validChunks)
		minID, min = minChunkserver(validChunks)
	}

	return nil
}

// Transfer a chunk from one chunkserver to another
// In the case of failure, this method *should* result of duplication
// of data, not loss of data
func (bal *balancer) transferSomeChunk(
	src apis.ServerID, dst apis.ServerID, chunks map[apis.ServerID]map[apis.ChunkVersion]bool) error {

	// Choose the mapping from the src chunkserver
	cvs, ok := chunks[src]
	if !ok {
		return errors.New("List of chunks does not contain source server")
	}
	// Grab some chunknum from the source to move
	var curCV apis.ChunkVersion
	var chunknum apis.ChunkNum
	chunknum = 0
	for cv, _ := range cvs {
		curCV = cv
		chunknum = cv.Chunk
	}
	if chunknum == 0 {
		return errors.New("Chunklist for that source server was zero")
	}

	entry, owner, err := bal.localCache.ReadEntry(chunknum)
	if owner != apis.NoRedirect {
		return fmt.Errorf("Metadata for this server currently leased by %v", owner)
	} else if err != nil {
		return err
	}

	csSrc, err := bal.idToCS(src)
	if err != nil {
		return err
	}
	dstAddr, err := chunkupdate.AddressForChunkserver(bal.etcd, dst)
	if err != nil {
		return err
	}

	// If this current function is halted, one of the replications
	// *should* be garbage collected.
	err = csSrc.Replicate(chunknum, dstAddr, entry.MostRecentVersion)
	if err != nil {
		return err
	}

	// Find index of source in replicas
	replicas := []apis.ServerID{}
	replicas = append(replicas, entry.Replicas...)

	repI := -1
	for i, id := range replicas {
		if src == id {
			repI = i
			break
		}
	}
	if repI == -1 {
		return fmt.Errorf("Source server %d could not be found in list of replicas for chunk %v", src, chunknum)
	}

	// Remove the src from the list of replicas and add the dst
	replicas = append(replicas[:repI], replicas[repI+1:]...)
	replicas = append(replicas, dst)

	owner, err = bal.localCache.UpdateEntry(chunknum, entry, apis.MetadataEntry{
		MostRecentVersion:   entry.MostRecentVersion,
		LastConsumedVersion: entry.LastConsumedVersion,
		Replicas:            replicas,
	})

	if owner != apis.NoRedirect {
		return fmt.Errorf("Cannot update metadata for chunk %d as server %d has a lease on it.", chunknum, owner)
	} else if err != nil {
		return err
	}

	// Finally, reflect the chunk move in the mapping of chunks provided
	delete(chunks[src], curCV)
	chunks[dst][curCV] = true

	return nil
}

// Generate a mapping of chunkserver to valid chunks that it currently contains
// This mapping would not contain the chunkservers or its chunks for any chunkserver that is down,
// and would not contain any chunks that the chunkserver somehow lost or has designated as invalid
// TODO Use abstraction magic to genericize this
func (bal *balancer) genValidChunks() (map[apis.ServerID]map[apis.ChunkVersion]bool, error) {
	chunkservers, err := chunkupdate.ListChunkservers(bal.etcd)
	if err != nil {
		return nil, err
	}

	// Map to chunk version, as a previous version of a chunk doesn't count for our replication goals
	chunks := make(map[apis.ServerID]map[apis.ChunkVersion]bool)
	for _, chunkserver := range chunkservers {
		// TODO Make sure this times out if the target is down
		cs, err := bal.idToCS(chunkserver)
		if err != nil {
			log.Printf("Server %s threw error: %v while constructing list of valid chunks", chunkserver, err)
			continue
		}

		// This assumes chunkserver to return only its valid chunks
		cvs, err := cs.ListAllChunks()
		if err != nil {
			log.Printf("Server %s threw error: %v while constructing list of valid chunks", chunkserver, err)
			continue
		}
		// Doing this as map instead of a list for faster lookup
		cvsMap := make(map[apis.ChunkVersion]bool)
		for _, cv := range cvs {
			cvsMap[cv] = true
		}
		chunks[chunkserver] = cvsMap
	}

	return chunks, nil
}

// Given a chunkserver id, return a connection to that chunkserver
func (bal *balancer) idToCS(id apis.ServerID) (apis.Chunkserver, error) {
	addr, err := chunkupdate.AddressForChunkserver(bal.etcd, id)
	if err != nil {
		return nil, err
	}

	return bal.rpcCache.SubscribeChunkserver(addr)
}

func minChunkserver(chunks map[apis.ServerID]map[apis.ChunkVersion]bool) (minID apis.ServerID, min int) {
	// TODO Fix this hack
	minID = 0
	min = MaxInt
	for serverID, chunkMap := range chunks {
		if len(chunkMap) < min {
			min = len(chunkMap)
			minID = serverID
		}
	}

	return minID, min
}

func maxChunkserver(chunks map[apis.ServerID]map[apis.ChunkVersion]bool) (maxID apis.ServerID, max int) {
	maxID = 0
	max = 0
	for serverID, chunkMap := range chunks {
		if len(chunkMap) > max {
			max = len(chunkMap)
			maxID = serverID
		}
	}

	return maxID, max
}
