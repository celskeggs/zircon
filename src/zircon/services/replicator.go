package services

import (
	"fmt"
	"log"
	"zircon/apis"
	"zircon/chunkupdate"
	"zircon/rpc"
)

const MinReplicas int = 2

// Explanation of the replication service:
//     Every chunk in the cluster should be replicated to at least two servers, preferably three.
//     The replication service goes through, counts valid replicas, and replicates new ones as necessary.
//         (TODO: have chunkservers periodically check their disk checksums)
func ReplicatorService(etcd apis.EtcdInterface, localCache apis.MetadataCache, rpcCache rpc.ConnectionCache) (cancel func() error, err error) {
	return nil, nil
}

type replicator struct {
	etcd       apis.EtcdInterface
	localCache apis.MetadataCache
	rpcCache   rpc.ConnectionCache
}

func (rpl *replicator) replicate() error {
	// Generate a list of valid chunk refences per chunkserver
	validChunks, err := rpl.genValidChunks()
	if err != nil {
		return err
	}

	blockIDs, err := rpl.etcd.ListAllMetadataBlocks()
	if err != nil {
		return err
	}

	for _, blockID := range blockIDs {
		// Check for valid replicatioon of metadata chunks
		// 1. Replace any chunk references that are not in our list of valid chunk references
		// 2. Make sure that the replication of each chunk is at least minReplication
		// 3. Replace chunk reference that somehow are not up-to-date with the current version

		// TODO This whole part. Poke cela about how metadata blocks are noew done

		// Check for valid replication of data chunks
		entries, owner, err := rpl.localCache.ReadAllEntries(blockID)
		if err == nil {
			rpl.replicateChunks(entries, validChunks)
		} else if owner != apis.NoRedirect {
			log.Printf("Server %s has lease on block %d. Skipping over it.", owner, blockID)
			continue
		} else {
			log.Printf("Cache threw error %v on block %d. Skipping over it.", err, blockID)
		}
	}

	return nil
}

// Generate a mapping of chunkserver to valid chunks that it currently contains
// This mapping would not contain the chunkservers or its chunks for any chunkserver that is down,
// and would not contain any chunks that the chunkserver somehow lost or has designated as invalid
func (rpl *replicator) genValidChunks() (map[apis.ServerID]map[apis.ChunkVersion]bool, error) {
	chunkservers, err := chunkupdate.ListChunkservers(rpl.etcd)
	if err != nil {
		return nil, err
	}

	// Map to chunk version, as a previous version of a chunk doesn't count for our replication goals
	chunks := make(map[apis.ServerID]map[apis.ChunkVersion]bool)
	for _, chunkserver := range chunkservers {
		cs, err := rpl.idToCS(chunkserver)
		if err != nil {
			log.Printf("Server %s threw error: %v while constructing list of valid chunks", chunkserver, err)
			continue
		}

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

// Given a list of entries and a list of valid ChunkVersions per chunkserver,
// ensure than each chunk is replicated to an appropriate number of healthy servers
func (rpl *replicator) replicateChunks(entries map[apis.ChunkNum]apis.MetadataEntry, validChunks map[apis.ServerID]map[apis.ChunkVersion]bool) {
	for chunk, entry := range entries {
		// TODO Is this the right version to use?
		cv := apis.ChunkVersion{
			Chunk:   chunk,
			Version: entry.MostRecentVersion,
		}

		// Designate each reference for this chunk as valid or invalid
		validReplicas := []apis.ServerID{}
		invalidReplicas := []apis.ServerID{}
		for _, serverID := range entry.Replicas {
			serverChunks, ok := validChunks[serverID]
			if !ok {
				invalidReplicas = append(invalidReplicas, serverID)
			}
			_, ok = serverChunks[cv]
			if ok {
				validReplicas = append(validReplicas, serverID)
			} else {
				invalidReplicas = append(validReplicas, serverID)
			}
		}

		// If all references are invalid, log that fact and be sad
		if len(validReplicas) == 0 {
			// TODO Maybe try to recover with a previous version
			log.Printf("Chunk %d not present on any available server. Could not be replicated", chunk)
			continue
		}

		// Just choose the first valid replica to replicate from
		source := validReplicas[0]

		// TODO Poss. do something better than just using the keys from the server to valid chunks mapping
		availServers := []apis.ServerID{}
		for id, _ := range validChunks {
			if id != source {
				availServers = append(availServers, id)
			}
		}

		var nReplicas int
		// Assure that the chunk is replicated at least MinReplica times
		if len(validReplicas)+len(invalidReplicas) < MinReplicas {
			nReplicas = MinReplicas - len(validReplicas)
		} else {
			nReplicas = len(invalidReplicas)
		}

		err := rpl.replicateChunk(chunk, entry, source, availServers, nReplicas)
		if err != nil {
			log.Printf("Replicating chunk %d from Server #%d threw err: %v", chunk, source, err)
			continue
		}
	}
}

// Replicate a given chunk from the source server to N of the servers given in availServer where N is nReplications
func (rpl *replicator) replicateChunk(chunk apis.ChunkNum, entry apis.MetadataEntry, source apis.ServerID, availServers []apis.ServerID, nReplications int) error {
	if nReplications < 0 {
		return fmt.Errorf("Replication factor is %d, less than 0", nReplications)
	}

	sourceCS, err := rpl.idToCS(source)
	if err != nil {
		return err
	}

	// Relying on chunk balancer to fix bad allocations patterns from this
	// TODO Possibly regenerate the pool of available chunkservers to choose from or limit to ones with space for new chunks

	newReplicas := []apis.ServerID{}
	for 0 < nReplications {
		if len(availServers) == 0 {
			log.Printf("Ran out of available servers for replication while replicating %d", chunk)
			break
		}

		repServer := availServers[0]
		availServers = availServers[1:]
		repName, err := rpl.etcd.GetNameByID(repServer)
		if err != nil {
			return err
		}

		repAddress, err := rpl.etcd.GetAddress(repName, apis.CHUNKSERVER)
		if err != nil {
			return err
		}

		// TODO Is this the right way to handle these versions
		err = sourceCS.Replicate(chunk, repAddress, entry.MostRecentVersion)
		if err != nil {
			log.Printf("When replicating chunk %d from Server #%d to Server #%d: %v", chunk, source, repServer)
			continue
		}

		newReplicas = append(newReplicas, repServer)
		nReplications -= 1
	}

	// Update the metadata entry with the new replicas
	_, err = rpl.localCache.UpdateEntry(chunk, entry, apis.MetadataEntry{
		MostRecentVersion:   entry.MostRecentVersion,
		LastConsumedVersion: entry.LastConsumedVersion,
		Replicas:            append(newReplicas, source),
	})

	return err
}

// Given a chunkserver id, return a connection to that chunkserver
func (rpl *replicator) idToCS(id apis.ServerID) (apis.Chunkserver, error) {
	addr, err := chunkupdate.AddressForChunkserver(rpl.etcd, id)

	return rpl.rpcCache.SubscribeChunkserver(addr)
}
