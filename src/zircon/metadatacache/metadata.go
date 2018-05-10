package metadatacache

import (
	"errors"
	"zircon/apis"
	"zircon/rpc"
	"zircon/metadatacache/leasing"
	"fmt"
	"zircon/util"
	"encoding/binary"
)

type metadatacache struct {
	leasing *leasing.Leasing
}

// Construct a new metadata cache.
func NewCache(connCache rpc.ConnectionCache, etcd apis.EtcdInterface) (apis.MetadataCache, error) {
	agent, err := leasing.ConstructLeasing(etcd, connCache)
	if err != nil {
		return nil, err
	}
	// TODO: figure out a good time to run Stop()
	// TODO: be able to automatically re-establish a lease by Stop()/Start() sequence
	err = agent.Start()
	if err != nil {
		return nil, err
	}

	return &metadatacache{
		leasing: agent,
	}, nil
}

// Reads the metadata entry of a particular chunk.
// Return the entry and if another server holds the block containing that entry, that server's name
func (mc *metadatacache) ReadEntry(chunk apis.ChunkNum) (apis.MetadataEntry, apis.ServerName, error) {
	metachunk, offset := chunkToBlockAndOffset(chunk)
	data, _, owner, err := mc.leasing.Read(metachunk)
	if err != nil {
		return apis.MetadataEntry{}, owner, err
	}

	found := getBitsetInData(data, chunkToEntryNumber(chunk))
	if !found {
		return apis.MetadataEntry{}, apis.NoRedirect, errors.New("entry doesn't exist to be able to be read")
	}

	entry, err := deserializeEntry(data[offset:offset+apis.EntrySize])
	if err != nil {
		return apis.MetadataEntry{}, apis.NoRedirect, err
	}

	return entry, apis.NoRedirect, nil
}

// Update the metadata entry of a particular chunk.
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) UpdateEntry(chunk apis.ChunkNum, previous apis.MetadataEntry, newEntry apis.MetadataEntry) (apis.ServerName, error) {
	metachunk, offset := chunkToBlockAndOffset(chunk)

	for {
		data, version, owner, err := mc.leasing.Read(metachunk)
		if err != nil {
			return owner, fmt.Errorf("[metadata.go/MLR] %v", err)
		}

		found := getBitsetInData(data, chunkToEntryNumber(chunk))
		if !found {
			return apis.NoRedirect, errors.New("entry doesn't exist to be able to be updated")
		}

		entry, err := deserializeEntry(data[offset:offset+apis.EntrySize])
		if err != nil {
			return apis.NoRedirect, fmt.Errorf("[metadata.go/DSE] %v", err)
		}
		if !entry.Equals(previous) {
			return apis.NoRedirect, errors.New("entry does not match previous expected entry")
		}

		updated, err := serializeEntry(newEntry)
		if err != nil {
			return apis.NoRedirect, fmt.Errorf("[metadata.go/SRE] %v", err)
		}
		if len(updated) != apis.EntrySize {
			panic("postcondition on serializeEntry failed")
		}

		_, owner, err = mc.leasing.Write(metachunk, version, offset, updated)
		if err == nil {
			// success!
			return apis.NoRedirect, nil
		} else if version == 0 {
			return owner, fmt.Errorf("[metadata.go/MLW] %v", err)
		}
		// version mismatch; go around again and re-attempt changes
	}
}

// Delete a metadata entry and allow the garbage collection of the underlying chunks
// If another server holds the block containing that entry, returns that server's name
func (mc *metadatacache) DeleteEntry(chunk apis.ChunkNum, previous apis.MetadataEntry) (apis.ServerName, error) {
	metachunk, offset := chunkToBlockAndOffset(chunk)

	for {
		data, version, owner, err := mc.leasing.Read(metachunk)
		if err != nil {
			return owner, err
		}

		found := getBitsetInData(data, chunkToEntryNumber(chunk))
		if !found {
			return apis.NoRedirect, errors.New("entry doesn't exist to be able to be deleted")
		}

		entry, err := deserializeEntry(data[offset:offset+apis.EntrySize])
		if err != nil {
			return apis.NoRedirect, err
		}
		if !entry.Equals(previous) {
			return apis.NoRedirect, errors.New("entry does not match previous expected entry")
		}

		updateOffset, newData := updateBitsetInData(data, chunkToEntryNumber(chunk), false)

		_, owner, err = mc.leasing.Write(metachunk, version, updateOffset, newData)
		if err == nil {
			return apis.NoRedirect, nil
		} else if version == 0 {
			return owner, err
		}
		// version mismatch; go around again and re-attempt changes
	}
}

// Deserialize a metadate entry using gob
func deserializeEntry(data []byte) (apis.MetadataEntry, error) {
	if len(util.StripTrailingZeroes(data)) == 0 {
		// no data to deserialize; represents empty metadata entry
		return apis.MetadataEntry{}, nil
	}

	var entry apis.MetadataEntry
	entry.MostRecentVersion = apis.Version(binary.LittleEndian.Uint64(data))
	entry.LastConsumedVersion = apis.Version(binary.LittleEndian.Uint64(data[8:]))
	entry.Replicas = make([]apis.ServerID, data[16])
	for i := 0; i < len(entry.Replicas); i++ {
		entry.Replicas[i] = apis.ServerID(binary.LittleEndian.Uint32(data[20 + 4 * i:]))
	}

	return entry, nil
}

// Serialize a metadata entry using gob
// Caps to a size that should be large enough, unless a ton of replicas are included
func serializeEntry(entry apis.MetadataEntry) ([]byte, error) {
	data := make([]byte, apis.EntrySize)
	binary.LittleEndian.PutUint64(data, uint64(entry.MostRecentVersion))
	binary.LittleEndian.PutUint64(data[8:], uint64(entry.LastConsumedVersion))
	if len(entry.Replicas) >= 256 || len(entry.Replicas) > (apis.EntrySize - 20) / 4 {
		return nil, fmt.Errorf("too many replicas: %d", len(entry.Replicas))
	}
	data[16] = uint8(len(entry.Replicas))
	for i := 0; i < len(entry.Replicas); i++ {
		binary.LittleEndian.PutUint32(data[20 + 4 * i:], uint32(entry.Replicas[i]))
	}

	return data, nil
}

// Compute the metadata block, and offset within the block, that a certain chunk belongs to
func chunkToBlockAndOffset(chunk apis.ChunkNum) (apis.MetadataID, uint32) {
	return chunkToBlockID(chunk), entryNumberToOffset(chunkToEntryNumber(chunk))
}

// Compute which metadata block the chunk belongs to
func chunkToBlockID(chunk apis.ChunkNum) apis.MetadataID {
	return apis.MetadataID(chunk >> apis.EntriesPerBlock)
}

// Compute the index within its metadata block where a chunk should be able to be found
func chunkToEntryNumber(chunk apis.ChunkNum) uint32 {
	// Extract just the lower bits
	return uint32(chunk) & ((1 << apis.EntriesPerBlock) - 1)
}

// Calculate the offset of the metadata entry inside of the block in bytes
func entryNumberToOffset(entryN uint32) uint32 {
	return uint32(entryN)*apis.EntrySize + apis.BitsetSize
}

func entryAndBlockToChunkNum(metachunk apis.MetadataID, index uint32) apis.ChunkNum {
	if metachunk == 0 || index >= (1 << apis.EntriesPerBlock) {
		panic("broken invariant for chunk location")
	}
	return apis.ChunkNum(uint64(metachunk << apis.EntriesPerBlock) | uint64(index))
}

// Allocate a new metadata entry and corresponding chunk number
func (mc *metadatacache) NewEntry() (apis.ChunkNum, error) {
	for {
		metachunk, index, err := mc.findAnyFreeChunk()
		if err != nil {
			return 0, fmt.Errorf("[metadata.go/FFC] %v", err)
		}

		noclobber, err := mc.updateBitset(metachunk, index, true)
		if err != nil {
			return 0, fmt.Errorf("[metadata.go/MUB] %v", err)
		}

		if noclobber {
			return entryAndBlockToChunkNum(metachunk, index), nil
		}
		// welp... guess we gotta go around again! someone else messed with our chunk.
	}
}

// Checks whether a chunk has been allocated or not in the bitset part of a certain metachunk.
func (mc *metadatacache) getBitset(metachunk apis.MetadataID, index uint32) (bool, error) {
	data, _, _, err := mc.leasing.Read(metachunk)
	if err != nil {
		return false, err
	}

	return getBitsetInData(data[0:apis.BitsetSize], index), nil
}

// Checks whether a chunk has been allocated or not in a bitset.
func getBitsetInData(bitset []byte, index uint32) (bool) {
	cell := bitset[index / 8]
	var mask byte = 1 << (index % 8)
	return (cell & mask) != 0
}

// Provides write parameters to update a bitset: (offset, data)
func updateBitsetInData(bitset []byte, index uint32, value bool) (uint32, []byte) {
	cell := bitset[index / 8]
	var mask byte = 1 << (index % 8)
	if value {
		cell |= mask
	} else {
		cell &= mask ^ 0xFF
	}
	return uint32(index / 8), []byte{ cell }
}

// Update whether a chunk has been allocated or not in the bitset part of a certain metachunk.
// Returns 'true' if the request succeeded, false if it was clobbered, and error if anything else happened.
func (mc *metadatacache) updateBitset(metachunk apis.MetadataID, index uint32, value bool) (bool, error) {
	data, version, _, err := mc.leasing.Read(metachunk)
	if err != nil {
		return false, err
	}

	bitset := data[0:apis.BitsetSize]

	existingValue := getBitsetInData(data[0:apis.BitsetSize], index)
	if existingValue == value {
		return false, nil
	}

	offset, newData := updateBitsetInData(bitset, index, value)

	_, _, err = mc.leasing.Write(metachunk, version, offset, newData)
	if err != nil {
		return false, err
	}

	return true, nil
}

// Tries to find a free chunk anywhere. Returns (metadataID, index, error)
func (mc *metadatacache) findAnyFreeChunk() (apis.MetadataID, uint32, error) {
	// First, see if there is an open spot in a lease that we hold
	metadataID, index, found, err := mc.findAnyLeasedFreeChunk()
	if err != nil {
		return 0, 0, fmt.Errorf("[metadata.go/FLC] %v", err)
	}
	if found {
		return metadataID, index, nil
	}

	// Now start trying everything else
	for {
		// TODO: what if two calls happen at once, and both get new metadata blocks? that's inefficient!
		// TODO: what if one server runs through this a lot of times, and suddenly has everything claimed? inefficient!
		metadataID, err := mc.leasing.GetOrCreateAnyUnleased()
		if err != nil {
			return 0, 0, fmt.Errorf("[metadata.go/GCU] %v", err)
		}

		index, found, err := mc.findFreeChunkIn(metadataID)
		if err != nil {
			return 0, 0, fmt.Errorf("[metadata.go/FCI] %v", err)
		}

		if found {
			return metadataID, index, nil
		}

		// not found; let's go around the loop again, and try to find ANOTHER unleased...
		// TODO: what if this goes around infinitely, due to a bug?
	}
}

// Tries to find a free chunk in a specific chunk. Returns (index, found, error)
func (mc *metadatacache) findFreeChunkIn(metachunk apis.MetadataID) (uint32, bool, error) {
	data, _, _, err := mc.leasing.Read(metachunk)
	if err != nil {
		return 0, false, err
	}

	cellIndex, found := findAvailableCell(data[0:apis.BitsetSize])
	if found {
		return cellIndex, true, nil
	}

	return 0, false, nil
}

// Tries to find a free chunk in one of our leased chunks. Returns (metadataID, index, found, error)
func (mc *metadatacache) findAnyLeasedFreeChunk() (apis.MetadataID, uint32, bool, error) {
	leases, err := mc.leasing.ListLeases()
	if err != nil {
		return 0, 0, false, err
	}
	for _, metachunk := range leases {
		index, found, err := mc.findFreeChunkIn(metachunk)
		if err != nil {
			return 0, 0, false, err
		}
		if found {
			return metachunk, index, true, nil
		}
	}
	return 0, 0, false, nil
}

// Finds a cell in a bitset that has a chunkNum available and returns the index of the available cell
func findAvailableCell(bitset []byte) (uint32, bool) {
	for i, cell := range bitset {
		// Cell is full if it is all ones
		if cell != 0xFF {
			return uint32(i) * 8 + findFirstZero(cell), true
		}
	}
	return 0, false
}

func findFirstZero(x byte) uint32 {
	for i := uint32(0); i < 8; i++ {
		if x&1 == 0 {
			return i
		} else {
			x = x >> 1
		}
	}
	panic("had no zeroes!")
}
