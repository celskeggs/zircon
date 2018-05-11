package filesystem

import (
	"zircon/apis"
	"errors"
)

type FilesystemSync struct {
	s apis.SyncServer
}

func (f *FilesystemSync) WriteLockChunk(chunk apis.ChunkNum) (Unlocker, error) {
	unlocker, err := f.ReadLockChunk(chunk)
	if err != nil {
		return Unlocker{}, err
	}
	defer unlocker.Unlock()
	unl2, err := unlocker.Elevate()
	if err != nil {
		return Unlocker{}, err
	}
	return unl2, nil
}

func (f *FilesystemSync) ReadLockChunk(chunk apis.ChunkNum) (Unlocker, error) {
	syncid, err := f.s.StartSync(chunk)
	if err != nil {
		return Unlocker{}, err
	}
	return Unlocker{
		syncid: syncid,
		s: f.s,
		active: true,
	}, nil
}

// note: the root chunk never changes
func (f *FilesystemSync) GetRoot() (apis.ChunkNum, error) {
	return f.s.GetFSRoot()
}

type Unlocker struct {
	s      apis.SyncServer
	syncid apis.SyncID
	active bool
}

func (u *Unlocker) Ensure() error {
	if !u.active {
		return errors.New("inactive sync")
	}
	_, err := u.s.ConfirmSync(u.syncid)
	return err
}

func (u *Unlocker) EnsureWrite() error {
	if !u.active {
		return errors.New("inactive sync")
	}
	write, err := u.s.ConfirmSync(u.syncid)
	if err != nil {
		return err
	}
	if !write {
		return errors.New("not a write lock")
	}
	return nil
}

func (u *Unlocker) Elevate() (Unlocker, error) {
	if !u.active {
		return Unlocker{}, errors.New("inactive sync")
	}
	nsync, err := u.s.UpgradeSync(u.syncid)
	if err != nil {
		return Unlocker{}, err
	}
	return Unlocker{
		syncid: nsync,
		s: u.s,
		active: true,
	}, nil
}

func (u *Unlocker) Unlock() {
	if u.active {
		u.active = false
		err := u.s.ReleaseSync(u.syncid)
		if err != nil {
			// TODO: this should probably just loop forever until the lock can actually be released
			panic("failed to unlock file!")
		}
	}
}
