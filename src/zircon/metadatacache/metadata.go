package metadatacache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
	"zircon/apis"
	"zircon/rpc"
)

type metadatacache struct {
	leases    map[apis.MetadataID]apis.Metametadata
	cache     map[apis.ChunkNum]apis.MetadataEntry
	mu        sync.Mutex // Lock to protect the cache
	connCache rpc.ConnectionCache
	etcd      apis.EtcdInterface
}

// Allocate a new metadata entry and corresponding chunk number
func (mc *metadatacache) NewEntry() (apis.ChunkNum, error) {
	chunk, metadata, err := mc.findFreeChunkBitset()
	if err != nil {
		return 0, err
	}
	err = mc.setBitset(true, metadata, chunk)
	if err != nil {
		return 0, err
	}

	return chunk, nil
}

// Reads the metadata entry of a particular chunk.
// Return the entry and if another server holds the block containing that entry, that server's name
func (mc *metadatacache) ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, apis.ServerName, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	entry, ok := mc.cache[chunk]

	if ok {
		return entry, apis.NO_REDIRECT, nil
	} else {
		// Get entry and place into cache
		// TODO: find a safe way to avoid keeping the lock through readThrough
		entry, owner, err := mc.readThrough(chunk)
		if err != nil {
			return apis.MetadataEntry{}, owner, err
		}
		mc.cache[chunk] = entry

		return entry, apis.NO_REDIRECT, nil
	}
}

// Update the metadate entry of a particular chunk.
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) UpdateEntry(chunk apis.ChunkNum, previous apis.MetadataEntry, entry apis.MetadataEntry) (apis.ServerName, error) {
	// TODO: do something with 'previous'
	owner, err := mc.writeThrough(chunk, entry)
	if err != nil {
		return owner, err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	// TODO: figure out how to lock earlier to preserve invariants
	mc.cache[chunk] = entry

	return apis.NO_REDIRECT, nil
}

// Delete a metadata entry and allow the garbage collection of the underlying chunks
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) DeleteEntry(chunk apis.ChunkNum) (apis.ServerName, error) {
	owner, err := mc.deleteThrough(chunk)
	if err != nil {
		return owner, err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	// TODO: figure out how to lock earlier to preserve invariants
	delete(mc.cache, chunk)

	return apis.NO_REDIRECT, nil
}

// Read a metadata entry from the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) readThrough(chunk apis.ChunkNum) (apis.MetadataEntry, apis.ServerName, error) {
	block, offset := chunkToBlockAndOffset(chunk)

	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return apis.MetadataEntry{}, owner, err
	}

	name, _ := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return apis.MetadataEntry{}, apis.NO_REDIRECT, err
	}

	// TODO: extract a separate block cache
	data, _, err := chunkserver.Read(metadata.Chunk, offset, apis.EntrySize, metadata.Version)
	if err != nil {
		return apis.MetadataEntry{}, apis.NO_REDIRECT, err
	}

	entry, err := deserializeEntry(data)
	if err != nil {
		return apis.MetadataEntry{}, apis.NO_REDIRECT, err
	}

	return entry, apis.NO_REDIRECT, nil
}

// Write an entry to the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) writeThrough(chunk apis.ChunkNum, entry apis.MetadataEntry) (apis.ServerName, error) {
	block := chunkToBlockID(chunk)
	offset := entryNumberToOffset(chunkToEntryNumber(chunk))

	data, err := serializeEntry(entry)
	if err != nil {
		return apis.NO_REDIRECT, err
	}

	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return owner, err
	}

	name, primaryI := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return apis.NO_REDIRECT, err
	}

	// Remove the name chosen to get the list of replicas
	var replicas []apis.ServerAddress
	for i, name := range metadata.Locations {
		if i != primaryI {
			replica, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
			if err != nil {
				return apis.NO_REDIRECT, err
			}
			replicas = append(replicas, replica)
		}
	}
	err = chunkserver.StartWriteReplicated(metadata.Chunk, offset, data, replicas)
	if err != nil {
		return apis.NO_REDIRECT, err
	}

	return apis.NO_REDIRECT, nil
}

// Delete a metadata entry from the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) deleteThrough(chunk apis.ChunkNum) (apis.ServerName, error) {
	block := chunkToBlockID(chunk)
	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return owner, err
	}

	// TODO: figure out the likely race conditions here

	mc.setBitset(false, metadata, chunk)

	return apis.NO_REDIRECT, nil
}

// Resolve a chunk server name to a connection with that server
func (mc *metadatacache) nameToConn(name apis.ServerName) (apis.Chunkserver, error) {
	addr, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
	if err != nil {
		return nil, err
	}

	return mc.connCache.SubscribeChunkserver(addr)
}

// Tries to find a free chunk in one of our leased chunks
func (mc *metadatacache) findLeasedFreeChunk() (uint64, uint, bool, apis.Metametadata, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for _, metadata := range mc.leases {
		bitset, err := mc.getBitset(metadata)
		if err != nil {
			return 0, 0, false, apis.Metametadata{}, err
		}
		cellI, cell, ok := findAvailableCell(bitset)
		if ok {
			return cell, cellI, true, metadata, nil
		}
	}
	return 0, 0, false, apis.Metametadata{}, nil
}

// Finds a free chunk somewhere, prioritizing currently-leased blocks.
func (mc *metadatacache) findFreeChunkBitset() (apis.ChunkNum, apis.Metametadata, error) {
	// First, see if there is an open spot in a lease that we hold
	cell, cellI, ok, metadataOut, err := mc.findLeasedFreeChunk()
	if err != nil {
		return 0, apis.Metametadata{}, err
	}

	// Now enumerate through the rest of the metablocks
	if !ok {
		freeBlockID, _, err := mc.etcd.LeaseSomeMetametadata()
		if err != nil {
			return 0, apis.Metametadata{}, err
		}

		metadata, _, err := mc.getMetadata(freeBlockID)
		if err != nil {
			return 0, apis.Metametadata{}, err
		}
		bitset, err := mc.getBitset(metadata)
		if err != nil {
			return 0, apis.Metametadata{}, err
		}

		metadataOut = metadata

		cellI, cell, ok = findAvailableCell(bitset)
		if !ok {
			panic("this should have succeeded!")
		}
	}

	// Should never return -1 as an error because of the earlier check
	bitI := findFirstZero(cell)
	// TODO: what does this 64 do?
	chunk := apis.ChunkNum(cellI*64 + uint(bitI))
	return chunk, metadataOut, nil
}

// Get the bitset corresponding to the chunks available to be allocated in that metadata block
func (mc *metadatacache) getBitset(metadata apis.Metametadata) ([]uint64, error) {
	name, _ := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return nil, err
	}

	data, _, err := chunkserver.Read(metadata.Chunk, 0, apis.BitsetSize, metadata.Version)
	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(bytes.NewReader(data))

	var bitset []uint64
	err = dec.Decode(&bitset)

	return bitset, err
}

// Finds a cell in the bitset that has a chunkNum available and returns the index of that cell and the cell
func findAvailableCell(bitset []uint64) (uint, uint64, bool) {
	for i, cell := range bitset {
		var mask uint64
		mask = 0xFFFFFFFFFFFFFFFF
		// Cell is full if it is all ones
		if cell != mask {
			return uint(i), cell, true
		}
	}

	return 0, 0, false
}

func findFirstZero(x uint64) int {
	for i := 0; i < 64; i++ {
		if x&1 == 0 {
			return i
		} else {
			x = x >> 1
		}
	}
	return -1
}

// Set whether a chunk is available to be allocated or not in the bitset
func (mc *metadatacache) setBitset(value bool, metadata apis.Metametadata, chunk apis.ChunkNum) error {
	bitset, err := mc.getBitset(metadata)
	if err != nil {
		return err
	}

	cellI := chunk >> 6
	var bitFieldSize uint = apis.ChunkNumSize - 6
	bitI := (chunk << bitFieldSize) >> bitFieldSize

	bitset[cellI] &= 1 << bitI

	data := bytes.NewBuffer(make([]byte, 0, apis.BitsetSize))
	enc := gob.NewEncoder(data)
	err = enc.Encode(bitset)
	if err != nil {
		return err
	}

	if len(metadata.Locations) == 0 {
		return errors.New("no metadata locations found")
	}

	name, primaryI := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return err
	}

	// Remove the name chosen to get the list of replicas
	var replicas []apis.ServerAddress
	for i, name := range metadata.Locations {
		if i != primaryI {
			replica, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
			if err != nil {
				return err
			}
			replicas = append(replicas, replica)
		}
	}

	// TODO: fix this so that it actually performs a write correctly
	return chunkserver.StartWriteReplicated(metadata.Chunk, 0, data.Bytes(), replicas)
}

// Choose a chunk server to connect to given a list of names
// For now, a random name is chosen
// Return also the index chosen, so it can be removed in certain cases
func chooseOneChunkserver(names []apis.ServerName) (apis.ServerName, int) {
	if len(names) == 0 {
		panic("No names to choose")
	}
	i := rand.Int() % len(names)
	return names[i], i
}

func (mc *metadatacache) getMetadata(block apis.MetadataID) (apis.Metametadata, apis.ServerName, error) {
	log.Printf("Get metadata for %d", block)
	// Check to see if we have lease on block already
	mc.mu.Lock()
	defer mc.mu.Unlock()
	metadata, ok := mc.leases[block]
	if ok {
		return metadata, apis.NO_REDIRECT, nil
	}

	// Otherwise try to obtain a lease
	owner, err := mc.etcd.TryClaimingMetadata(block)
	if err != nil {
		return apis.Metametadata{}, owner, err
	}

	// Then obtain the metametadata
	metadata, err = mc.etcd.GetMetametadata(block)
	if err != nil {
		return apis.Metametadata{}, apis.NO_REDIRECT, err
	}

	// If the metametadata is unitialized, do that
	if len(metadata.Locations) == 0 {
		metadata, err = mc.initMetadata(block)
		if err != nil {
			return apis.Metametadata{}, apis.NO_REDIRECT, err
		}
	}
	// TODO: find a way to have dropped the lock before here, so that we don't hold it too long
	mc.leases[block] = metadata

	if metadata.MetaID != block {
		return apis.Metametadata{}, apis.NO_REDIRECT, errors.New("MetadataID doesn't match stored metadata")
	}
	if len(metadata.Locations) == 0 {
		return apis.Metametadata{}, apis.NO_REDIRECT, errors.New("metadata has no locations")
	}

	return metadata, apis.NO_REDIRECT, nil
}

func (mc *metadatacache) initMetadata(block apis.MetadataID) (apis.Metametadata, error) {
	// TODO Replicate to more than one chunkserver
	chunkservers, err := mc.etcd.ListServers(apis.CHUNKSERVER)
	if err != nil {
		return apis.Metametadata{}, err
	}
	if len(chunkservers) == 0 {
		return apis.Metametadata{}, errors.New("no chunkservers available")
	}
	chunkserver := chunkservers[rand.Int()%len(chunkservers)]
	chunkNum, err := mc.initBlock(chunkserver, block)
	if err != nil {
		return apis.Metametadata{}, err
	}
	mmd := apis.Metametadata{
		MetaID:    block,
		Chunk:     chunkNum,
		Version:   1, // TODO: select the first version better
		Locations: []apis.ServerName{chunkserver},
	}

	err = mc.etcd.UpdateMetametadata(block, mmd)
	return mmd, err
}

func (mc *metadatacache) initBlock(cs apis.ServerName, block apis.MetadataID) (apis.ChunkNum, error) {
	chunk := entryAndBlockToChunkNum(0, block)

	bitset := make([]uint64, apis.BitsetSize/8)
	entry := apis.MetadataEntry{}

	bitbuf := new(bytes.Buffer)
	enc := gob.NewEncoder(bitbuf)
	err := enc.Encode(bitset)
	if err != nil {
		return 0, err
	}
	bitbytes := bitbuf.Bytes()

	entrybytes, err := serializeEntry(entry)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, apis.MaxChunkSize)
	copy(buf[:len(bitbytes)], bitbytes)
	for i := 0; i < 1<<apis.EntriesPerBlock; i++ {
		offset := apis.BitsetSize + i*apis.EntrySize
		copy(buf[offset:offset+len(entrybytes)], entrybytes)
	}

	conn, err := mc.nameToConn(cs)
	if err != nil {
		return 0, err
	}
	err = conn.Add(chunk, buf, 1)

	return chunk, err
}

// Service for periodically renewing metametadata leases
// If it fails to renew leases, flush the entire cache
func (mc *metadatacache) leaseRenewal() {
	// TODO: figure out a better way to handle this whole 'lease' thing
	renewalTimeout := mc.etcd.GetMetadataLeaseTimeout() / time.Duration(4)
	for {
		err := mc.etcd.RenewMetadataClaims()
		if err != nil {
			mc.flushCache()
		}

		time.Sleep(renewalTimeout)
	}
}

// Empty all leases and all cached entries
func (mc *metadatacache) flushCache() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.leases = make(map[apis.MetadataID]apis.Metametadata)
	mc.cache = make(map[apis.ChunkNum]apis.MetadataEntry)
}

// Compute the metadata block, and offset within the block, that a certain chunk belongs to
func chunkToBlockAndOffset(chunk apis.ChunkNum) (apis.MetadataID, uint32) {
	return chunkToBlockID(chunk), entryNumberToOffset(chunkToEntryNumber(chunk))
}

// Compute which metadata block the chunk belongs to
func chunkToBlockID(chunk apis.ChunkNum) apis.MetadataID {
	return apis.MetadataID(chunk >> apis.EntriesPerBlock)
}

// Compute the offset within its metadata block where a chunk should be able to be found
func chunkToEntryNumber(chunk apis.ChunkNum) uint32 {
	// Calculate the number of bits corresponding the block number
	var blockBits uint32 = apis.ChunkNumSize - apis.EntriesPerBlock
	// Zero out those bits
	entryNumber := (chunk << blockBits) >> blockBits

	return uint32(entryNumber)
}

// Calculate the offset of the metadata entry inside of the block in bytes
func entryNumberToOffset(entryN uint32) uint32 {
	return entryN*apis.EntrySize + apis.BitsetSize
}

func entryAndBlockToChunkNum(entryN uint, block apis.MetadataID) apis.ChunkNum {
	// TODO: figure out what this '15' is
	return apis.ChunkNum(uint(block)<<15 + entryN)
}

// TODO: have serialize/deserialized implemented for bitsets too

// Serialize a metadata entry using gob
// Caps to a size that should be large enough, unless a ton of replicas are included
func serializeEntry(entry apis.MetadataEntry) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, apis.EntrySize))
	enc := gob.NewEncoder(buf)
	err := enc.Encode(entry)

	return buf.Bytes(), err
}

// Unserialize a metadate entry using gob
func deserializeEntry(data []byte) (apis.MetadataEntry, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var entry apis.MetadataEntry
	err := dec.Decode(&entry)

	return entry, err
}

func NewCache(connCache rpc.ConnectionCache, etcd apis.EtcdInterface) (apis.MetadataCache, error) {
	err := etcd.BeginMetadataLease()
	if err != nil {
		return nil, err
	}

	mc := metadatacache{
		leases:    make(map[apis.MetadataID]apis.Metametadata),
		cache:     make(map[apis.ChunkNum]apis.MetadataEntry),
		connCache: connCache,
		etcd:      etcd,
	}

	// Start goroutine to periodically renew metametadata leases
	// TODO: have way to kill it
	go mc.leaseRenewal()

	return &mc, nil
}
