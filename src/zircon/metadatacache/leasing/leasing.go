package leasing

import (
	"zircon/metadatacache/access"
	"zircon/apis"
	"zircon/rpc"
	"sync"
	"github.com/pkg/errors"
	"fmt"
	"time"
)

type Lease struct {
	// TODO: lease-level locking
	Version  apis.Version
	Contents []byte
}

type Leasing struct {
	access *access.Access
	etcd   apis.EtcdInterface

	mu        sync.Mutex
	cancel    chan struct{}
	done      chan struct{}
	safe      bool
	leases    map[apis.MetadataID]*Lease
}

func ConstructLeasing(etcd apis.EtcdInterface, cache rpc.ConnectionCache) (*Leasing, error) {
	chunkAccess, err := access.ConstructAccess(etcd, cache)
	if err != nil {
		return nil, err
	}
	return &Leasing{
		access: chunkAccess,
		etcd: etcd,
		leases: make(map[apis.MetadataID]*Lease),
	}, nil
}

func (l *Leasing) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cancel != nil {
		return errors.New("cannot start leasing agent; already running!")
	}
	if l.done != nil {
		return errors.New("cannot start leasing agent; not done stopping!")
	}

	err := l.etcd.BeginMetadataLease()
	if err != nil {
		return err
	}

	l.cancel = make(chan struct{})
	l.done = make(chan struct{})
	l.safe = true

	go l.mainloop()

	return l.ensureRenewed_LK()
}

func (l *Leasing) Stop() error {
	done := func() chan struct{} {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.cancel == nil {
			return nil
		}
		close(l.cancel)
		l.cancel = nil
		if l.done == nil {
			panic("done should not be nil yet!")
		}
		return l.done
	}()
	if done == nil {
		return errors.New("either already stopped or already in the process of stopping!")
	}
	<-done
	l.mu.Lock()
	defer l.mu.Unlock()
	l.done = nil
	return nil
}

func (l *Leasing) mainloop() {
	defer func() {
		close(l.done)
	}()
	for {
		select {
		case <-l.cancel:
			break
		case <-time.After(l.etcd.GetMetadataLeaseTimeout() / 3):
			err := l.etcd.RenewMetadataClaims()
			if err != nil {
				l.notifyUnsafe()
				break
			}
		}
	}
}

func (l *Leasing) notifyUnsafe() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.notifyUnsafe_LK()
}

func (l *Leasing) ensureRenewed_LK() error {
	if err := l.etcd.RenewMetadataClaims(); err != nil {
		l.notifyUnsafe_LK()
		return err
	}
	return nil
}

func (l *Leasing) notifyUnsafe_LK() {
	l.safe = false
	l.leases = make(map[apis.MetadataID]*Lease)
}

// Get *any* unleased block. If everything that exists is leased, create a new block with all zeroes and lease it.
func (l *Leasing) GetOrCreateAnyUnleased() (apis.MetadataID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.safe {
		return 0, errors.New("lease has expired! cannot safely perform any operations.")
	}
	id, err := l.etcd.LeaseAnyMetametadata()
	if err != nil {
		return 0, err
	}
	var ver apis.Version
	var data []byte
	if id == 0 {
		// TODO: what if we lose our lease right here?
		id, err = l.access.New()
		if err != nil {
			return 0, err
		}
		// we do an empty write to make sure the block sticks around (TODO: is this necessary?)
		ver, err = l.access.Write(id, apis.AnyVersion, 0, []byte{})
		if err != nil {
			return 0, err
		}
		data = make([]byte, apis.MaxChunkSize)
	} else {
		data, ver, err = l.access.Read(id)
		if err != nil {
			return 0, err
		}
	}
	l.leases[id] = &Lease{
		Version: ver,
		Contents: data,
	}
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return 0, err
	}
	return id, nil
}

func (l *Leasing) populateCache_LK(metachunk apis.MetadataID) (*Lease, apis.ServerName, error) {
	if !l.safe {
		return nil, apis.NoRedirect, errors.New("lease has expired! cannot safely perform any operations.")
	}
	// try to claim the chunk
	owner, err := l.etcd.TryClaimingMetadata(metachunk)
	if err != nil {
		return nil, apis.NoRedirect, err
	}
	if owner != l.etcd.GetName() {
		return nil, owner, fmt.Errorf("owned by someone else: %s", owner)
	}

	// if we claimed it, cache and return it
	// TODO: don't block on reading this
	data, version, err := l.access.Read(metachunk)
	if err != nil {
		err2 := l.etcd.DisclaimMetadata(metachunk)
		if err2 != nil {
			err = fmt.Errorf("two errors: %v [[[ and, while cleaning up ]]] %v", err, err2)
		}
		return nil, apis.NoRedirect, err
	}
	l.leases[metachunk] = &Lease{
		Contents: data,
		Version: version,
	}
	return l.leases[metachunk], apis.NoRedirect, nil
}

func (l *Leasing) ListLeases() ([]apis.MetadataID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result []apis.MetadataID
	for k, _ := range l.leases {
		result = append(result, k)
	}
	return result, l.ensureRenewed_LK()
}

// Reads a complete chunk.
func (l *Leasing) Read(metachunk apis.MetadataID) ([]byte, apis.Version, apis.ServerName, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lease, found := l.leases[metachunk]
	if !found {
		// CACHE MISS
		foundLease, owner, err := l.populateCache_LK(metachunk)
		if err != nil {
			return nil, 0, owner, err
		}
		lease = foundLease
	}
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return nil, 0, apis.NoRedirect, err
	}
	// defensive copy; TODO avoid needing to do this
	result := make([]byte, apis.MaxChunkSize)
	copy(result, lease.Contents)
	return result, lease.Version, apis.NoRedirect, nil
}

// Writes part of a chunk. Only performs the write if the version matches. Returns the new version on success, or the
// old version on failure, if the problem was that the version was a mismatch. The returned version is zero on failure
// iff the problem was something else.
func (l *Leasing) Write(metachunk apis.MetadataID, version apis.Version, offset uint32, data []byte) (apis.Version, apis.ServerName, error) {
	if offset + uint32(len(data)) > apis.MaxChunkSize {
		return 0, apis.NoRedirect, errors.New("write is too large!")
	}
	if version == 0 {
		return 0, apis.NoRedirect, errors.New("version cannot be zero to Leasing.Write")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lease, found := l.leases[metachunk]
	if !found {
		foundLease, owner, err := l.populateCache_LK(metachunk)
		if err != nil {
			return 0, owner, err
		}
		lease = foundLease
	}
	if lease.Version != version {
		return lease.Version, apis.NoRedirect, errors.New("version mismatch during lease write")
	}
	// write through cache
	newVersion, err := l.access.Write(metachunk, version, offset, data)
	if err != nil {
		// note: we don't pass through checking about the version, because there should not have been any contention for
		// the latest version!
		return 0, apis.NoRedirect, err
	}
	// update cache
	copy(lease.Contents[offset:], data)
	lease.Version = newVersion
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return 0, apis.NoRedirect, err
	}
	return newVersion, apis.NoRedirect, nil
}
