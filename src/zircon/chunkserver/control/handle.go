package control

import (
	"zircon/chunkserver/storage"
	"zircon/apis"
	"sync"
	"fmt"
	"errors"
)

// a nullary function to tear down any internal state of a ChunkserverSingle instance
type Teardown func()

type commit struct {
	Offset apis.Offset
	Data   []byte
}

// an implementation of apis.ChunkserverSingle
type chunkserver struct {
	mu      sync.Mutex
	Storage storage.ChunkStorage
	Hashes  map[apis.CommitHash]commit
}

// This includes most of the chunkserver implementation; which it exports through the ChunkserverSingle interface, based
// on just a storage layer.
func ExposeChunkserver(storage storage.ChunkStorage) (apis.ChunkserverSingle, Teardown, error) {
	cs := &chunkserver{
		Storage: storage,
		Hashes: map[apis.CommitHash]commit{},
	}
	// TODO: RECOVERY PROCESS
	return cs, cs.Teardown, nil
}

func checkInvariantSameChunks(a []apis.ChunkNum, b []apis.ChunkNum) {
	if len(a) != len(b) {
		panic("violated invariant: expected both chunk lists to have identical elements")
	}
	found := map[apis.ChunkNum]bool{}
	for _, element := range a {
		if alreadyFound := found[element]; alreadyFound {
			panic("violated invariant: duplicate element of chunk list")
		}
		found[element] = true
	}
	for _, element := range b {
		if matchFound := found[element]; !matchFound {
			panic("violated invariant: unexpected element of chunk list")
		}
		delete(found, element)
	}
}

func (cs *chunkserver) ListAllChunks() ([]struct{ Chunk apis.ChunkNum; Version apis.Version }, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	var result []struct {Chunk apis.ChunkNum; Version apis.Version}
	latestChunks, err := cs.Storage.ListChunksWithLatest()
	if err != nil {
		return nil, err
	}
	existingChunks, err := cs.Storage.ListChunksWithData()
	if err != nil {
		return nil, err
	}
	checkInvariantSameChunks(latestChunks, existingChunks)
	for _, chunk := range existingChunks {
		versions, err := cs.Storage.ListVersions(chunk)
		if err != nil {
			return nil, err
		}
		versionExpected, err := cs.Storage.GetLatestVersion(chunk)
		if err != nil {
			return nil, err
		}
		foundExpected := false
		for _, version := range versions {
			result = append(result, struct {
				Chunk   apis.ChunkNum
				Version apis.Version
			}{Chunk: chunk, Version: version})
			if version == versionExpected {
				foundExpected = true
			}
		}
		if !foundExpected {
			panic("violated invariant: expected latest version to be present in list of actual versions")
		}
	}
	return result, nil
}

func (cs *chunkserver) Add(chunk apis.ChunkNum, initialData []byte, initialVersion apis.Version) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if initialVersion <= 0 {
		return fmt.Errorf("initial version was not positive: %d/%d", chunk, initialVersion)
	}

	versions, err := cs.Storage.ListVersions(chunk)
	if err != nil {
		return err
	}
	if len(versions) > 0 {
		return fmt.Errorf("attempt to create duplicate chunk: %d/%d", chunk, initialVersion)
	}
	err = cs.Storage.WriteVersion(chunk, initialVersion, initialData)
	if err != nil {
		return err
	}
	err = cs.Storage.SetLatestVersion(chunk, initialVersion)
	if err != nil {
		err2 := cs.Storage.DeleteVersion(chunk, initialVersion)
		if err2 != nil {
			panic("failed to be able to maintain invariant") // TODO: handle this more gracefully than crashing
		}
		return err
	}
	return nil
}

func (cs *chunkserver) Delete(chunk apis.ChunkNum, version apis.Version) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if version <= 0 {
		return fmt.Errorf("deleted version was not positive: %d/%d", chunk, version)
	}

	latest, err := cs.Storage.GetLatestVersion(chunk)
	if err != nil {
		return err
	}

	// if we delete the latest version, we also delete everything newer... and because nothing older will exist at this
	// point, we delete everything.
	if latest == version {
		versions, err := cs.Storage.ListVersions(chunk)
		if err != nil {
			return err
		}
		// mark the entire chunk as able to be deleted
		if err := cs.Storage.DeleteLatestVersion(chunk); err != nil {
			return err
		}
		// then delete all versions of the chunk
		for _, delver := range versions {
			if err := cs.Storage.DeleteVersion(chunk, delver); err != nil {
				return err
			}
		}
	} else {
		// just delete the single version
		if err := cs.Storage.DeleteVersion(chunk, version); err != nil {
			return err
		}
	}
	return nil
}

func (cs *chunkserver) Teardown() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// wipe away any pending hashes
	// TODO: have a way to regularly wipe away stale pending hashes
	cs.Hashes = map[apis.CommitHash]commit{}
}

// Given a chunk reference, read out part or all of a chunk.
// If 'minimum' is AnyVersion, then whichever version the chunkserver currently has will be returned.
// If the version of the chunk that this chunkserver has is at least the minimum version, it will be returned.
// Otherwise, an error will be returned, along with the most recent available version.
// The sum of offset + length must not be greater than MaxChunkSize. The number of bytes returned is always exactly
// the same number of bytes requested if there is no error.
// The version of the data actually read will be returned.
// Fails if a copy of this chunk isn't located on this chunkserver.
func (cs *chunkserver) Read(chunk apis.ChunkNum, offset apis.Offset, length apis.Length, minimum apis.Version) ([]byte, apis.Version, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if apis.Length(offset) + length > apis.MaxChunkSize {
		return nil, 0, errors.New("too much data")
	}

	version, err := cs.Storage.GetLatestVersion(chunk)
	if err != nil {
		return nil, 0, err
	}
	if version < minimum {
		return nil, version, errors.New("requested newer version than was available")
	}
	data, err := cs.Storage.ReadVersion(chunk, version)
	if err != nil {
		return nil, version, err
	}
	result := make([]byte, length)
	realEnd := int(offset) + int(length)
	if realEnd > len(data) {
		realEnd = len(data)
	}
	if realEnd > int(offset) {
		copy(result, data[offset:realEnd])
	}
	return result, version, nil
}

// Given a chunk reference, send data to be used for a write to this chunk.
// This method does not actually perform a write.
// The sum of 'offset' and 'len(data)' must not be greater than MaxChunkSize.
// Fails if a copy of this chunk isn't located on this chunkserver.
func (cs *chunkserver) StartWrite(chunk apis.ChunkNum, offset apis.Offset, data []byte) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	_, err := cs.Storage.GetLatestVersion(chunk)
	if err != nil {
		return err
	}

	if int(offset) + len(data) > int(apis.MaxChunkSize) {
		return errors.New("too much data to write")
	}

	cs.Hashes[apis.CalculateCommitHash(offset, data)] = struct {
		Offset apis.Offset
		Data   []byte
	}{Offset: offset, Data: data}

	return nil
}

// Commit a write -- persistently store it as the data for a particular version.
// Takes existing saved data for oldVersion, apply this cached write, and saved it as newVersion.
func (cs *chunkserver) CommitWrite(chunk apis.ChunkNum, hash apis.CommitHash, oldVersion apis.Version, newVersion apis.Version) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if newVersion <= oldVersion {
		return errors.New("cannot rewrite history")
	}

	latest, err := cs.Storage.GetLatestVersion(chunk)
	if err != nil {
		return err
	}

	if latest != oldVersion {
		return fmt.Errorf("attempt to write to mismatched version (%d/%d -> %d/%d) when latest is %d/%d",
			chunk, oldVersion, chunk, newVersion, chunk, latest)
	}

	write, found := cs.Hashes[hash]
	if !found {
		return errors.New("could not locate write by commit hash")
	}

	data, err := cs.Storage.ReadVersion(chunk, oldVersion)
	if err != nil {
		return err
	}

	dataLen := int(write.Offset) + len(write.Data)
	if dataLen < len(data) {
		dataLen = len(data)
	}
	if dataLen > int(apis.MaxChunkSize) {
		panic("invariant broken: length of block should never exceed MaxChunkSize")
	}

	newData := make([]byte, dataLen)
	copy(newData, data)
	copy(newData[write.Offset:], write.Data)

	return cs.Storage.WriteVersion(chunk, newVersion, newData)
}

// Update the version of this chunk that will be returned to clients. (Also allowing this chunkserver to delete
// older versions.)
// If the specified chunk does not exist on this chunkserver, errors.
// If the current version reported to clients is different from the oldVersion, errors.
func (cs *chunkserver) UpdateLatestVersion(chunk apis.ChunkNum, oldVersion apis.Version, newVersion apis.Version) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if newVersion <= oldVersion {
		return errors.New("cannot rewrite history")
	}

	latest, err := cs.Storage.GetLatestVersion(chunk)
	if err != nil {
		return err
	}
	if latest != oldVersion {
		return fmt.Errorf("attempt to update to mismatched version (%d/%d -> %d/%d) when latest is %d/%d",
			chunk, oldVersion, chunk, newVersion, chunk, latest)
	}

	// TODO: have an api to just check, rather than needing to iterate
	versions, err := cs.Storage.ListVersions(chunk)
	if err != nil {
		return err
	}
	found := false
	for _, ver := range versions {
		found = found || (ver == newVersion)
	}
	if !found {
		return fmt.Errorf("no write found for version: %d/%d", chunk, newVersion)
	}

	// change the latest version
	if err := cs.Storage.SetLatestVersion(chunk, newVersion); err != nil {
		return err
	}

	// TODO: be able to recover from a failure in here

	// eliminate everything older
	for _, ver := range versions {
		if ver < newVersion {
			if err := cs.Storage.DeleteVersion(chunk, ver); err != nil {
				return err
			}
		}
	}

	return nil
}
