package metadata

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
	entry, ok := mc.cache[chunk]
	mc.mu.Unlock()

	if ok {
		return entry, "", nil
	} else {
		// Get entry and place into cache
		entry, owner, err := mc.readThrough(chunk)
		if err != nil {
			return apis.MetadataEntry{}, owner, err
		}
		mc.mu.Lock()
		mc.cache[chunk] = entry
		mc.mu.Unlock()

		return entry, "", nil
	}
}

// Update the metadate entry of a particular chunk.
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) UpdateEntry(chunk apis.ChunkNum, entry apis.MetadataEntry) (apis.ServerName, error) {
	owner, err := mc.writeThrough(chunk, entry)
	if err != nil {
		return owner, err
	}

	mc.mu.Lock()
	mc.cache[chunk] = entry
	mc.mu.Unlock()

	return "", nil
}

// Delete a metadata entry and allow the garbage collection of the underlying chunks
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) DeleteEntry(chunk apis.ChunkNum) (apis.ServerName, error) {
	owner, err := mc.deleteThrough(chunk)
	if err != nil {
		return owner, err
	}

	mc.mu.Lock()
	delete(mc.cache, chunk)
	mc.mu.Unlock()

	return "", nil
}

// Read a metadata entry from the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) readThrough(chunk apis.ChunkNum) (apis.MetadataEntry, apis.ServerName, error) {
	block := chunkToBlockID(chunk)
	offset := entryNumberToOffset(chunkToEntryNumber(chunk))

	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return apis.MetadataEntry{}, owner, err
	}

	name, _ := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return apis.MetadataEntry{}, "", err
	}

	data, _, err := chunkserver.Read(metadata.Chunk, offset, apis.EntrySize, metadata.Version)
	if err != nil {
		return apis.MetadataEntry{}, "", err
	}

	entry, err := deserializeEntry(data)
	if err != nil {
		return apis.MetadataEntry{}, "", err
	}

	return entry, "", nil
}

// Write an entry to the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) writeThrough(chunk apis.ChunkNum, entry apis.MetadataEntry) (apis.ServerName, error) {
	block := chunkToBlockID(chunk)
	offset := entryNumberToOffset(chunkToEntryNumber(chunk))

	data, err := serializeEntry(entry)
	if err != nil {
		return "", err
	}

	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return owner, err
	}

	name, primaryI := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return "", err
	}

	// Remove the name chosen to get the list of replicas
	replicas := []apis.ServerAddress{}
	for i, name := range metadata.Locations {
		if i != primaryI {
			replica, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
			if err != nil {
				return "", err
			}
			replicas = append(replicas, replica)
		}
	}
	err = chunkserver.StartWriteReplicated(metadata.Chunk, offset, data, replicas)
	if err != nil {
		return "", err
	}

	return "", nil
}

// Delete a metadata entry from the backing store
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) deleteThrough(chunk apis.ChunkNum) (apis.ServerName, error) {
	block := chunkToBlockID(chunk)
	metadata, owner, err := mc.getMetadata(block)
	if err != nil {
		return owner, err
	}

	mc.setBitset(false, metadata, chunk)

	return "", nil
}

// Resolve a chunk server name to a connection with that server
func (mc *metadatacache) nameToConn(name apis.ServerName) (apis.Chunkserver, error) {
	addr, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
	if err != nil {
		return nil, nil
	}

	cs, err := mc.connCache.SubscribeChunkserver(addr)
	return cs, err
}

func (mc *metadatacache) findFreeChunkBitset() (apis.ChunkNum, apis.Metametadata, error) {

	mc.mu.Lock()
	ok := false
	var cell uint64
	var cellI uint
	var metadata apis.Metametadata
	// First, see if there is an open spot in a lease that we hold
	for _, metadata := range mc.leases {
		log.Printf("Grabbing a lease")
		bitset, err := mc.getBitset(metadata)
		if err != nil {
			return 0, apis.Metametadata{}, err
		}
		_cellI, _cell, ok := findAvailableCell(bitset)
		cellI = _cellI
		cell = _cell
		if ok {
			break
		}
	}
	mc.mu.Unlock()

	// Now enumerate through the rest of the metablocks
	log.Printf("Start alt search")
	if !ok {
		for {
			freeBlockID, _, err := mc.etcd.LeaseSomeMetametadata()
			if err != nil {
				return 0, apis.Metametadata{}, err
			}

			log.Printf("Get meta")
			metadata, _, err := mc.getMetadata(freeBlockID)
			if err != nil {
				log.Printf("step 3")
				return 0, apis.Metametadata{}, err
			}
			log.Printf("Get bitset")
			bitset, err := mc.getBitset(metadata)
			if err != nil {
				return 0, apis.Metametadata{}, err
			}

			_cellI, _cell, ok := findAvailableCell(bitset)
			cellI = _cellI
			cell = _cell
			if ok {
				break
			}
			// Should succeed first time, so this for now
			log.Printf("err: %v", err)
			panic("nope")
		}
	}
	log.Printf("Finish alt search")

	// Should never return -1 as an error because of the earlier check
	bitI := findFirstZero(cell)
	chunk := apis.ChunkNum(cellI<<6 + uint(bitI))
	return chunk, metadata, nil
}

// Get the bitset corresponding to the chunks available to be allocated in that metadata block
func (mc *metadatacache) getBitset(metadata apis.Metametadata) ([]uint64, error) {
	log.Printf("Metadata %v", metadata)

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
		return nil
	}

	if len(metadata.Locations) == 0 {
	}

	name, primaryI := chooseOneChunkserver(metadata.Locations)
	chunkserver, err := mc.nameToConn(name)
	if err != nil {
		return err
	}

	// Remove the name chosen to get the list of replicas
	replicas := []apis.ServerAddress{}
	for i, name := range metadata.Locations {
		if i != primaryI {
			replica, err := mc.etcd.GetAddress(name, apis.CHUNKSERVER)
			if err != nil {
				return err
			}
			replicas = append(replicas, replica)
		}
	}
	err = chunkserver.StartWriteReplicated(metadata.Chunk, 0, data.Bytes(), replicas)
	if err != nil {
		return err
	}

	return nil
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
	metadata, ok := mc.leases[block]
	mc.mu.Unlock()
	if ok {
		log.Printf("Block %d was in cache as %v", block, metadata)
		return metadata, "", nil
	}
	log.Printf("Block %d was not in cache", block)

	// Otherwise try to obtain a lease
	owner, err := mc.etcd.TryClaimingMetadata(block)
	if err != nil {
		return apis.Metametadata{}, owner, err
	}

	// Then obtain the metametadata
	metadata, err = mc.etcd.GetMetametadata(block)
	if err != nil {
		return apis.Metametadata{}, "", err
	}

	// If the metametadata is unitialized, do that
	if len(metadata.Locations) == 0 {
		metadata, err = mc.initMetadata(block)
		if err != nil {
			log.Printf("step2")
			return apis.Metametadata{}, "", err
		}
	}
	log.Printf("Got to this point. Block is %v", metadata)
	mc.mu.Lock()
	mc.leases[block] = metadata
	mc.mu.Unlock()

	if metadata.MetaID != block {
		return apis.Metametadata{}, "", errors.New("MetadataID doesn't match stored Metadata")
	}
	if len(metadata.Locations) == 0 {
		return apis.Metametadata{}, "", errors.New("Metadata has no locations")
	}

	return metadata, "", nil
}

func (mc *metadatacache) initMetadata(block apis.MetadataID) (apis.Metametadata, error) {
	// TODO Replicate to more than one chunkserver
	chunkservers, err := mc.etcd.ListServers(apis.CHUNKSERVER)
	if err != nil {
		return apis.Metametadata{}, err
	}
	if len(chunkservers) == 0 {
		return apis.Metametadata{}, errors.New("No chunkservers available")
	}
	chunkserver := chunkservers[rand.Int()%len(chunkservers)]
	chunkNum, err := mc.initBlock(chunkserver, block)
	if err != nil {
		return apis.Metametadata{}, err
	}
	mmd := apis.Metametadata{
		MetaID:    block,
		Chunk:     chunkNum,
		Version:   1,
		Locations: []apis.ServerName{chunkserver},
	}

	err = mc.etcd.UpdateMetametadata(block, mmd)
	log.Printf("here it is %v", mmd)
	return mmd, err
}

func (mc *metadatacache) initBlock(cs apis.ServerName, block apis.MetadataID) (apis.ChunkNum, error) {
	log.Printf("init block")
	chunk := entryAndBlockToChunkNum(0, block)

	bitset := make([]uint64, apis.BitsetSize/8)
	entry := apis.MetadataEntry{}

	bitbuf := new(bytes.Buffer)
	enc := gob.NewEncoder(bitbuf)
	err := enc.Encode(bitset)
	if err != nil {
		return chunk, err
	}
	bitbytes := bitbuf.Bytes()

	entrybuf := new(bytes.Buffer)
	enc = gob.NewEncoder(entrybuf)
	err = enc.Encode(entry)
	if err != nil {
		return chunk, err
	}
	entrybytes := bitbuf.Bytes()

	buf := make([]byte, apis.MaxChunkSize)
	copy(buf[:len(bitbytes)], bitbytes)
	for i := 0; i < 1<<apis.EntriesPerBlock; i++ {
		offset := apis.BitsetSize + i*apis.EntrySize
		copy(buf[offset:offset+len(entrybytes)], entrybytes)
	}

	conn, err := mc.nameToConn(cs)
	if err != nil {
		return chunk, err
	}
	err = conn.Add(chunk, buf, 1)

	return chunk, err
}

// Service for periodically renewing metametadata leases
// If it fails to renew leases, flush the entire cache
func (mc *metadatacache) leaseRenewal() {
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
	mc.leases = make(map[apis.MetadataID]apis.Metametadata)
	mc.cache = make(map[apis.ChunkNum]apis.MetadataEntry)
	mc.mu.Unlock()
}

// Compute which metadata block the chunk belongs to
func chunkToBlockID(chunk apis.ChunkNum) apis.MetadataID {
	return apis.MetadataID(chunk >> apis.EntriesPerBlock)
}

// Compute which metadata block the chunk belongs to
func chunkToEntryNumber(chunk apis.ChunkNum) uint {
	// Calculate the number of bits corresponding the block number
	var blockBits uint = apis.ChunkNumSize - apis.EntriesPerBlock
	// Zero out those bits
	entryNumber := (chunk << blockBits) >> blockBits

	return uint(entryNumber)
}

// Calculate the offset of the metadata entry inside of the block in bytes
func entryNumberToOffset(entryN uint) apis.Offset {
	return apis.Offset(entryN*apis.EntrySize + apis.BitsetSize)
}

func entryAndBlockToChunkNum(entryN uint, block apis.MetadataID) apis.ChunkNum {
	return apis.ChunkNum(uint(block)<<15 + entryN)
}

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
	go mc.leaseRenewal()

	return &mc, nil
}
