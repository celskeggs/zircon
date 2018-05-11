package apis

type SyncID uint64

// syncserver methods that are the same in etcd and from the client's perspective
type SyncServerDirect interface {
	// Acquires a read lock on a certain chunk
	StartSync(chunk ChunkNum) (SyncID, error)

	// Derives a write lock from a read lock on a certain chunk
	UpgradeSync(s SyncID) (SyncID, error)

	// Releases a lock on a chunk
	ReleaseSync(s SyncID) error

	// Confirms that a sync is still valid -- remember that this has race conditions; avoid its usage
	ConfirmSync(s SyncID) (write bool, err error)
}

// TODO: we can probably associate some metadata with acquired locks, so that a server can recover its previous operations
// i.e. list and identify its old syncs, fetch client metadata
type SyncServer interface {
	SyncServerDirect

	// Gets the root chunk used by the filesystem. NEVER zero. NEVER changes.
	GetFSRoot() (ChunkNum, error)
}
