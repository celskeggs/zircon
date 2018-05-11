package fuse

import (
	"zircon/filesystem"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse"
	"io"
)

type fuseFile struct {
	base filesystem.WritableFile
}

var _ nodefs.File = &fuseFile{}

func (f *fuseFile) SetInode(*nodefs.Inode) {
	// do nothing
}

func (f *fuseFile) String() string {
	return "unspecified file"
}

func (f *fuseFile) InnerFile() nodefs.File {
	return nil
}

func (f *fuseFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	n, err := f.base.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, errorToFuseStatus(err)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func (f *fuseFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	n, err := f.base.WriteAt(data, off)
	return uint32(n), errorToFuseStatus(err)
}

func (f *fuseFile) Flock(flags int) fuse.Status {
	return fuse.ENOSYS
}

func (f *fuseFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *fuseFile) Release() {
	if f.base.Close() != nil {
		// Close() doesn't ever return non-nil errors for our thing
		panic("should never be non-nil error!")
	}
}

func (f *fuseFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

func (f *fuseFile) Truncate(size uint64) fuse.Status {
	return errorToFuseStatus(f.base.Truncate(size))
}

func (f *fuseFile) GetAttr(out *fuse.Attr) fuse.Status {
	// TODO
	return fuse.ENOSYS
}

func (f *fuseFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *fuseFile) Chmod(perms uint32) fuse.Status {
	return fuse.ENOSYS
}

func (f *fuseFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ENOSYS
}

func (f *fuseFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.ENOSYS
}
