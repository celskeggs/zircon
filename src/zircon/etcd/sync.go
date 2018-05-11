package etcd

import (
	"zircon/apis"
	"encoding/binary"
	"fmt"
	"context"
	"github.com/coreos/etcd/clientv3"
	"errors"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// This represents one of a few different states:
//    WriterSolitary(w) -- one client has a write lock. no one else may access this.
//    WriterRead(w, r) -- one client has a write lock and a read lock. no one else may access this.
//    Establishing(r, r[]) -- one client is trying to elevate its read lock to write status; no one new may access this.
//    Readers(r[]) -- everyone is just reading
//    Unlocked -- nobody is even reading
type syncLock struct {
	Writer         apis.SyncID
	IsWritePending bool
	Readers        []apis.SyncID
}

func (s syncLock) IsWriter() bool {
	return s.Writer != NoSync && !s.IsWritePending
}

func (s syncLock) HasOtherReaders() bool {
	return len(s.Readers) != 0
}

func (s syncLock) IsElevating() bool {
	return s.IsWritePending
}

func (s syncLock) IsReaders() bool {
	return s.Writer == NoSync && !s.IsWritePending && len(s.Readers) != 0
}

func (s syncLock) HasReader(sy apis.SyncID) bool {
	for _, reader := range s.Readers {
		if reader == sy {
			return true
		}
	}
	return false
}

func (s syncLock) IsUnlocked() bool {
	return s.Writer == NoSync && !s.IsWritePending && len(s.Readers) == 0
}

func (s syncLock) WithNewReader(reader apis.SyncID) syncLock {
	if s.Writer != NoSync || s.IsWritePending {
		panic("cannot add reader to this!")
	}
	nreaders := make([]apis.SyncID, len(s.Readers) + 1)
	copy(nreaders, s.Readers)
	nreaders[len(nreaders) - 1] = reader
	return syncLock{
		Readers: nreaders,
	}
}

func (s syncLock) WithoutReader(sync apis.SyncID) syncLock {
	if !s.HasReader(sync) {
		panic("WithoutReader expects presence of sync")
	}
	nreaders := make([]apis.SyncID, 0, len(s.Readers) - 1)
	for _, reader := range s.Readers {
		if reader != sync {
			nreaders = append(nreaders, reader)
		}
	}
	// this is fine because of immutability
	s.Readers = nreaders
	return s
}

func (s syncLock) WithoutWriter(sync apis.SyncID) syncLock {
	if s.Writer != sync {
		panic("WithoutWriter expects writer to match!")
	}
	s.Writer = NoSync
	s.IsWritePending = false
	return s
}

func (s syncLock) WithElevationAttempt(sync apis.SyncID) syncLock {
	if !s.HasReader(sync) {
		panic("WithElevationAttempt expects presence of sync")
	}
	if !s.IsReaders() {
		panic("WithElevationAttempt expects Readers mode")
	}
	s = s.WithoutReader(sync)
	s.IsWritePending = true
	s.Writer = sync
	return s
}

func (s syncLock) AsWriter(sync apis.SyncID) syncLock {
	if !s.IsElevating() || s.HasOtherReaders() || s.Writer == NoSync {
		panic("bad initial state for AsWriter!")
	}
	s.IsWritePending = false
	s.Readers = []apis.SyncID{s.Writer}
	s.Writer = sync
	return s
}

func decodeLockRaw(data []byte) (syncLock, error) {
	if len(data) < 11 {
		return syncLock{}, errors.New("data is too short to decode")
	}
	writer := apis.SyncID(binary.LittleEndian.Uint64(data))
	pending := data[8] != 0
	readers := make([]apis.SyncID, binary.LittleEndian.Uint16(data[9:11]))
	data = data[11:]
	if len(data) < 8 * len(readers) {
		return syncLock{}, errors.New("data is too short to decode after header")
	} else if len(data) > 8 * len(readers) {
		return syncLock{}, errors.New("data is too long to decode after header")
	}
	for i := 0; i < len(readers); i++ {
		readers[i] = apis.SyncID(binary.LittleEndian.Uint64(data[i*8:i*8+8]))
	}
	return syncLock{
		Writer: writer,
		IsWritePending: pending,
		Readers: readers,
	}, nil
}

func (s syncLock) encodeLockRaw() ([]byte, error) {
	if len(s.Readers) >= 65536 {
		return nil, errors.New("too many readers to encode")
	}
	result := make([]byte, 11 + len(s.Readers) * 8)
	binary.LittleEndian.PutUint64(result, uint64(s.Writer))
	if s.IsWritePending {
		result[8] = 1
	} else {
		result[8] = 0
	}
	binary.LittleEndian.PutUint16(result[9:], uint16(len(s.Readers)))
	for i, reader := range s.Readers {
		binary.LittleEndian.PutUint64(result[11 + i * 8:11 + i * 8 + 8], uint64(reader))
	}
	return result, nil
}

func decodeLockFromKV(kvs []*mvccpb.KeyValue) (syncLock, error) {
	if len(kvs) == 0 {
		return syncLock{}, nil
	} else {
		return decodeLockRaw(kvs[0].Value)
	}
}

func decodeLockLookup(c *clientv3.Client, chunk apis.ChunkNum) (syncLock, error) {
	chunkKey := fmt.Sprintf("/fs/lock/%d", chunk)
	resp, err := c.Get(context.Background(), chunkKey)
	if err != nil {
		return syncLock{}, err
	}
	return decodeLockFromKV(resp.Kvs)
}

func (s syncLock) encodeLockAsUpdate(chunk apis.ChunkNum) (clientv3.Op, error) {
	chunkKey := fmt.Sprintf("/fs/lock/%d", chunk)
	if s.IsUnlocked() {
		return clientv3.OpDelete(chunkKey), nil
	} else {
		enc, err := s.encodeLockRaw()
		if err != nil {
			return clientv3.Op{}, err
		}
		return clientv3.OpPut(chunkKey, string(enc)), nil
	}
}

func (s syncLock) encodeLockAsCompare(chunk apis.ChunkNum) (clientv3.Cmp, error) {
	chunkKey := fmt.Sprintf("/fs/lock/%d", chunk)
	if s.IsUnlocked() {
		return clientv3.Compare(clientv3.CreateRevision(chunkKey), "=", 0), nil
	} else {
		enc, err := s.encodeLockRaw()
		if err != nil {
			return clientv3.Cmp{}, err
		}
		return clientv3.Compare(clientv3.Value(chunkKey), "=", string(enc)), nil
	}
}

func watchSyncCancelable(c *clientv3.Client, chunk apis.ChunkNum) (context.CancelFunc, clientv3.WatchChan) {
	chunkKey := fmt.Sprintf("/fs/lock/%d", chunk)
	ctx, cancel := context.WithCancel(context.Background())
	return cancel, c.Watcher.Watch(ctx, chunkKey)
}

// calls f() repeatedly until it returns true; after the first call, waits for the lock key to change before continuing
func (e *etcdinterface) watchLoop(chunk apis.ChunkNum, f func() (bool, error)) error {
	chunkKey := fmt.Sprintf("/fs/lock/%d", chunk)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch := e.Client.Watcher.Watch(ctx, chunkKey)
	for {
		pass, err := f()
		if pass || err != nil {
			return err
		}

		// not in a helpful state; wait for something to change
		// TODO: think about failure modes re: client timeouts, crashes
		resp, ok := <-watch
		if resp.Canceled || !ok {
			err := resp.Err()
			if err == nil {
				err = errors.New("unknown watch failure")
			}
			return err
		}

		// got something! go around again.
	}
}

// returns success of the transaction
func rewriteSyncState(c *clientv3.Client, chunk apis.ChunkNum, prev syncLock, next syncLock, extra... clientv3.Op) (bool, error) {
	check, err := prev.encodeLockAsCompare(chunk)
	if err != nil {
		return false, err
	}
	nops := make([]clientv3.Op, len(extra) + 1)
	copy(nops[1:], extra)
	nops[0], err = next.encodeLockAsUpdate(chunk)
	if err != nil {
		return false, err
	}
	resp, err := c.Txn(context.Background()).If(check).Then(nops...).Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}

// Acquires a read lock on a certain chunk
func (e *etcdinterface) StartSync(chunk apis.ChunkNum) (apis.SyncID, error) {
	// Algorithm:
	//    WAIT until lock is Readers or Unlocked
	//    THEN add self to list of readers

	// get a syncid ready beforehand
	syncid, err := e.nextSyncID()
	if err != nil {
		return NoSync, err
	}

	syncKey := fmt.Sprintf("/fs/sync/%d", syncid)

	return syncid, e.watchLoop(chunk, func() (bool, error) {
		for {
			// we fetch the current state
			sl, err := decodeLockLookup(e.Client, chunk)
			if err != nil {
				return false, err
			}
			// if someone else is writing or trying to write, we gotta wait for them
			if !sl.IsUnlocked() && !sl.IsReaders() {
				return false, nil // wait for them
			}
			success, err := rewriteSyncState(e.Client, chunk, sl, sl.WithNewReader(syncid),
				clientv3.OpPut(syncKey, encodeChunk(chunk))) // this extra OpPut makes sure we also register the chunk name
			if err != nil {
				return false, err
			} else if success {
				return true, nil // report success
			}
			// not added due to conflict; let's try again
		}
	})
}

func (e *etcdinterface) getSyncChunk(s apis.SyncID) (apis.ChunkNum, error) {
	syncKey := fmt.Sprintf("/fs/sync/%d", s)

	resp, err := e.Client.Get(context.Background(), syncKey)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, errors.New("no such syncid")
	}
	return decodeChunk(resp.Kvs[0].Value), nil
}

// Derives a write lock from a read lock on a certain chunk. Errors if someone else is already trying to elevate.
func (e *etcdinterface) UpgradeSync(s apis.SyncID) (apis.SyncID, error) {
	// Algorithm:
	//    IF currently Readers(), then we move to Elevating.
	//    OTHERWISE abort
	//    WAIT until this is the only Elevating entry
	//    THEN move to Writer

	newsync, err := e.nextSyncID()
	if err != nil {
		return 0, err
	}
	newSyncKey := fmt.Sprintf("/fs/sync/%d", newsync)
	chunk, err := e.getSyncChunk(s)
	if err != nil {
		return 0, err
	}
	// FIRST STAGE -- MOVE TO Elevating
	for {
		// now we fetch the current status
		sl, err := decodeLockLookup(e.Client, chunk)
		if err != nil {
			return 0, err
		}
		if !sl.IsReaders() {
			if sl.IsUnlocked() {
				return 0, errors.New("should already hold a read lock when trying to upgrade")
			} else {
				return 0, errors.New("lock access contended")
			}
		}
		if !sl.HasReader(s) {
			return 0, errors.New("should already hold a read lock when trying to upgrade")
		}
		success, err := rewriteSyncState(e.Client, chunk, sl, sl.WithElevationAttempt(s))
		if err != nil {
			return 0, err
		}
		if success {
			// we were added successfully!
			break
		}
		// not added due to conflict; let's go around again
	}

	// SECOND STAGE -- MOVE TO Writer
	err = e.watchLoop(chunk, func() (bool, error) {
		for {
			// we fetch the current state
			sl, err := decodeLockLookup(e.Client, chunk)
			if err != nil {
				return false, err
			}
			// if someone else is writing or trying to write, we gotta wait for them
			if !sl.IsElevating() {
				return false, errors.New("elevation aborted")
			}
			if sl.HasOtherReaders() {
				return false, nil // wait some more
			}
			success, err := rewriteSyncState(e.Client, chunk, sl, sl.AsWriter(newsync),
				clientv3.OpPut(newSyncKey, encodeChunk(chunk))) // this extra OpPut makes sure we also register the chunk name
			if err != nil {
				return false, err
			} else if success {
				return true, nil // report success
			}
			// not added due to conflict; let's try again
		}
	})
	if err != nil {
		panic("needs to drop elevation here") // TODO
		return 0, err
	}
	return newsync, nil
}

// Releases a lock on a chunk
func (e *etcdinterface) ReleaseSync(s apis.SyncID) error {
	syncKey := fmt.Sprintf("/fs/sync/%d", s)
	chunk, err := e.getSyncChunk(s)
	if err != nil {
		return err
	}
	// FIRST STAGE -- MOVE TO Elevating
	for {
		// now we fetch the current status
		sl, err := decodeLockLookup(e.Client, chunk)
		if err != nil {
			return err
		}
		if sl.IsUnlocked() {
			panic("should never be unlocked here!")
		}
		var nsl syncLock
		if sl.HasReader(s) {
			nsl = sl.WithoutReader(s)
		} else if sl.IsWriter() || sl.IsElevating() {
			nsl = sl.WithoutWriter(s)
		} else {
			panic("this should never happen!")
		}
		success, err := rewriteSyncState(e.Client, chunk, sl, nsl,
			clientv3.OpDelete(syncKey))
		if err != nil {
			return err
		}
		if success {
			// we were removed successfully!
			return nil
		}
		// not removed due to conflict; let's go around again
	}
}

// Confirms that a sync is still valid -- remember that this has race conditions; avoid its usage
func (e *etcdinterface) ConfirmSync(s apis.SyncID) (write bool, err error) {
	chunk, err := e.getSyncChunk(s)
	if err != nil {
		return false, err
	}
	sl, err := decodeLockLookup(e.Client, chunk)
	if err != nil {
		return false, err
	}
	if !sl.HasReader(s) && sl.Writer != s {
		return false, errors.New("could not find sync")
	}
	return sl.Writer == s, nil
}

const FilesystemRootKey = "/fs/root"

func (e *etcdinterface) ReadFSRoot() (apis.ChunkNum, error) {
	resp, err := e.Client.Get(context.Background(), FilesystemRootKey)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	} else {
		return apis.ChunkNum(binary.LittleEndian.Uint64(resp.Kvs[0].Value)), nil
	}
}

func (e *etcdinterface) WriteFSRoot(chunk apis.ChunkNum) (error) {
	nchunk := make([]byte, 8)
	binary.LittleEndian.PutUint64(nchunk, uint64(chunk))
	resp, err := e.Client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(FilesystemRootKey), "=", 0)).
		Then(clientv3.OpPut(FilesystemRootKey, string(nchunk))).
		Commit()
	if err != nil {
		return err
	} else if !resp.Succeeded {
		return errors.New("found existing filesystem root")
	} else {
		return nil
	}
}
