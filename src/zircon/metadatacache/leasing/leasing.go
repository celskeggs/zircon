package leasing

import (
	"zircon/metadatacache/access"
	"zircon/apis"
	"zircon/rpc"
	"sync"
	"errors"
	"fmt"
	"time"
)

type Lease struct {
	// TODO: lease-level locking
	Version         apis.Version
	Contents        []byte
	WriteCompletion chan struct{}
}

type Leasing struct {
	access *access.Access
	etcd   apis.EtcdInterface

	mu         sync.Mutex
	cancel     chan struct{}
	done       chan struct{}
	safe       bool
	validUntil time.Time
	leases     map[apis.MetadataID]*Lease
	populating map[apis.MetadataID]chan struct{}
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
		populating: make(map[apis.MetadataID]chan struct{}),
	}, nil
}

func (l *Leasing) Start() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.cancel != nil {
		return errors.New("cannot start leasing agent; already running")
	}
	if l.done != nil {
		return errors.New("cannot start leasing agent; not done stopping")
	}

	start := time.Now()
	err := l.etcd.BeginMetadataLease()
	if err != nil {
		return err
	}

	l.cancel = make(chan struct{})
	l.done = make(chan struct{})
	l.safe = true
	l.validUntil = start.Add(l.etcd.GetMetadataLeaseTimeout())

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
			return
		case <-time.After(l.etcd.GetMetadataLeaseTimeout() / 3):
			start := time.Now()
			err := l.etcd.RenewMetadataClaims()
			if !time.Now().Before(l.validUntil) {
				// took too long, and we may have been considered to have lost leases
				// so now we just terminate.
				return
			}
			if err != nil {
				l.notifyUnsafe()
				return
			} else {
				l.validUntil = start.Add(l.etcd.GetMetadataLeaseTimeout())
			}
		}
	}
}

func (l *Leasing) notifyUnsafe() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.safe = false
	l.leases = make(map[apis.MetadataID]*Lease)
}

func (l *Leasing) ensureRenewed_LK() error {
	if time.Now().Before(l.validUntil) {
		return nil
	} else {
		return errors.New("lease timed out")
	}
}

func (l *Leasing) ensureClaimed(id apis.MetadataID) (apis.ServerName, error) {
	l.mu.Lock()
	_, foundLease := l.leases[id]
	_, foundPopulate := l.populating[id]
	l.mu.Unlock()
	if !foundLease && !foundPopulate {
		return l.etcd.TryClaimingMetadata(id)
	} else {
		return l.etcd.GetName(), nil
	}
}

func (l *Leasing) requestPopulation(id apis.MetadataID) error {
	// ATTEMPT TO REGISTER OURSELVES AS POPULATING
	l.mu.Lock()
	for l.populating[id] != nil {
		c := l.populating[id]
		l.mu.Unlock()
		<-c   // wait until they're done populating this
		l.mu.Lock()
		if l.populating[id] == c {
			l.populating[id] = nil
		}
	}
	if l.leases[id] != nil {
		// someone else already populated this!
		l.mu.Unlock()
		return nil
	} else {
		// our turn to try
		populateChan := make(chan struct{})
		defer close(populateChan)
		l.populating[id] = populateChan
		l.mu.Unlock()
		// FETCH DATA FOR POPULATION
		data, version, err := l.access.Read(id)
		if err != nil {
			return err
		}
		// POPULATE DATA
		l.mu.Lock()
		if l.leases[id] != nil {
			panic("nobody else should have touched this lease!")
		}
		l.leases[id] = &Lease{
			Contents: data,
			Version: version,
		}
		l.mu.Unlock()
		// we notify everyone at this point by closing the channel
		return nil
	}
}

// Get *any* unleased block. If everything that exists is leased, create a new block with all zeroes and lease it.
func (l *Leasing) GetOrCreateAnyUnleased() (apis.MetadataID, error) {
	// TODO: figure out how this handles leases if they're re-established during this time
	id, err := l.etcd.LeaseAnyMetametadata()
	if err != nil {
		return 0, fmt.Errorf("[leasing.go/LAM] %v", err)
	}
	if id == 0 {
		// TODO: what if we lose our lease right here?
		id, err = l.access.New()
		if err != nil {
			return 0, fmt.Errorf("[leasing.go/ACN] %v", err)
		}
		// we do an empty write to make sure the block sticks around (TODO: is this necessary?)
		// since the write is empty, there is no negative effect from it applying in the wrong scenario
		_, err = l.access.Write(id, apis.AnyVersion, 0, []byte{})
		if err != nil {
			return 0, fmt.Errorf("[leasing.go/ACW] %v", err)
		}
		owner, err := l.ensureClaimed(id)
		if err != nil {
			return 0, err
		}
		if owner != l.etcd.GetName() {
			return 0, errors.New("should not have been able to be claimed by someone else immediately after New()")
		}
	}
	if err := l.requestPopulation(id); err != nil {
		return 0, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return 0, err
	}
	return id, nil
}

func (l *Leasing) populateCache(id apis.MetadataID) (apis.ServerName, error) {
	// try to claim the chunk
	owner, err := l.ensureClaimed(id)
	if err != nil {
		return apis.NoRedirect, err
	}
	if owner != l.etcd.GetName() {
		return owner, fmt.Errorf("owned by someone else: %s", owner)
	}
	if err := l.requestPopulation(id); err != nil {
		return apis.NoRedirect, err
	}
	return apis.NoRedirect, nil
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
	owner, err := l.populateCache(metachunk)
	if err != nil {
		return nil, 0, owner, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	lease := l.leases[metachunk]
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return nil, 0, apis.NoRedirect, err
	}
	return lease.Contents, lease.Version, apis.NoRedirect, nil
}

// Writes part of a chunk. Only performs the write if the version matches. Returns the new version on success, or the
// old version on failure, if the problem was that the version was a mismatch. The returned version is zero on failure
// iff the problem was something else.
func (l *Leasing) Write(metachunk apis.MetadataID, version apis.Version, offset uint32, data []byte) (apis.Version, apis.ServerName, error) {
	if offset + uint32(len(data)) > apis.MaxChunkSize {
		return 0, apis.NoRedirect, errors.New("write is too large")
	}
	if version == 0 {
		return 0, apis.NoRedirect, errors.New("version cannot be zero to Leasing.Write")
	}
	owner, err := l.populateCache(metachunk)
	if err != nil {
		return 0, owner, err
	}
	l.mu.Lock()
	lease := l.leases[metachunk]
	if lease.Version != version {
		l.mu.Unlock()
		return lease.Version, apis.NoRedirect, errors.New("version mismatch during lease write")
	}
	for lease.WriteCompletion != nil {
		waitOn := lease.WriteCompletion
		l.mu.Unlock()
		<-waitOn
		l.mu.Lock()
		lease = l.leases[metachunk]
		if lease.WriteCompletion == waitOn {
			lease.WriteCompletion = nil
		}
		if lease.Version != version {
			l.mu.Unlock()
			return lease.Version, apis.NoRedirect, errors.New("version mismatch during lease write")
		}
	}
	writeChan := make(chan struct{})
	defer close(writeChan)
	lease.WriteCompletion = writeChan
	l.mu.Unlock()
	// write through cache
	newVersion, err := l.access.Write(metachunk, version, offset, data)
	if err != nil {
		// note: we don't pass through checking about the version, because there should not have been any contention for
		// the latest version!
		return 0, apis.NoRedirect, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// update cache
	updated := make([]byte, apis.MaxChunkSize)
	copy(updated, lease.Contents)
	copy(updated[offset:], data)
	lease.Contents = updated
	lease.Version = newVersion
	if err := l.ensureRenewed_LK(); err != nil {
		// cache invalidated!
		return 0, apis.NoRedirect, err
	}
	return newVersion, apis.NoRedirect, nil
}
