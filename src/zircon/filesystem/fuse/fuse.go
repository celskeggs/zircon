package fuse

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"zircon/filesystem"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"log"
	"zircon/apis"
	"path"
	"os"
)

type fuseFS struct {
	pathfs.FileSystem
	fs     filesystem.Filesystem
}

func NewFuseFS(fs filesystem.Filesystem) *fuseFS {
	return &fuseFS{
		fs: fs,
		FileSystem: pathfs.NewDefaultFileSystem(),
	}
}

var _ pathfs.FileSystem = &fuseFS{}

// Used for pretty printing.
func (f *fuseFS) String() string {
	return "Zircon Filesystem"
}

func errorToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	log.Printf("NOTE: providing default EIO result for error %v\n", err)
	return fuse.EIO
}

	// Attributes.  This function is the main entry point, through
	// which FUSE discovers which files and directories exist.
	//
	// If the filesystem wants to implement hard-links, it should
	// return consistent non-zero FileInfo.Ino data.  Using
	// hardlinks incurs a performance hit.
func (f *fuseFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	finfo, err := f.fs.Stat(name)
	if err != nil {
		return nil, errorToFuseStatus(err)
	}
	var links uint32 = 1
	if finfo.IsDir() {
		// TODO: don't do all of this just for a link count
		entries, err := f.fs.ListDir(name)
		if err != nil {
			return nil, errorToFuseStatus(err)
		}
		links++
		for _, ent := range entries {
			s, err := f.fs.Stat(path.Join(name, ent))
			if err != nil {
				return nil, errorToFuseStatus(err)
			}
			if s.IsDir() {
				links++
			}
		}
	}
	relTime := uint64(finfo.ModTime().Unix())
	return &fuse.Attr{
		Size: uint64(finfo.Size()),
		Atime: relTime,
		Ctime: relTime,
		Mtime: relTime,
		Blksize: apis.MaxChunkSize - 4,
		Blocks: 1,
		Mode: uint32(finfo.Mode()),
		Nlink: links,
		Owner: context.Owner,
	}, fuse.OK
}

func (f *fuseFS) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	if size > 0xFFFFFFFF {
		return fuse.ERANGE
	}
	return errorToFuseStatus(f.fs.Truncate(name, uint32(size)))
}

	// Tree structure
func (f *fuseFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	return errorToFuseStatus(f.fs.Mkdir(name))
}

func (f *fuseFS) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	return errorToFuseStatus(f.fs.Rename(oldName, newName))
}

func (f *fuseFS) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return errorToFuseStatus(f.fs.Rmdir(name))
}

func (f *fuseFS) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	return errorToFuseStatus(f.fs.Unlink(name))
}

	// Called after mount.
func (f *fuseFS) OnMount(nodeFs *pathfs.PathNodeFs) {
	// do nothing
}

func (f *fuseFS) OnUnmount() {
	// do nothing
}

	// File handling.  If opening for writing, the file's mtime
	// should be updated too.
func (f *fuseFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	if (int(flags) & os.O_APPEND) != 0 {

	}
	if (int(flags) & os.O_CREATE) != 0 {

	}
	exclusive := (int(flags) & os.O_EXCL) != 0
	if (int(flags) & os.O_TRUNC) != 0 {

	}
	if (int(flags) & (os.O_WRONLY | os.O_RDWR)) == (os.O_WRONLY | os.O_RDWR) {
		return nil, fuse.EINVAL
	}
	writable := (int(flags) & (os.O_WRONLY)) != 0 || (int(flags) & (os.O_RDWR)) != 0
	var file filesystem.WritableFile
	var err error
	if writable {
		file, err = f.fs.OpenWrite(name, exclusive)
		if err != nil {
			return nil, errorToFuseStatus(err)
		}
	} else {
		subfile, err := f.fs.OpenRead(name)
		if err != nil {
			return nil, errorToFuseStatus(err)
		}
		file = filesystem.WithErroringWrite(subfile)
	}
	return &fuseFile{
		base: file,
	}, fuse.OK
}

func (f *fuseFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	return f.Open(name, flags | uint32(os.O_CREATE) | uint32(os.O_TRUNC) | uint32(os.O_WRONLY), context)
}

	// Directory handling
func (f *fuseFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	names, err := f.fs.ListDir(name)
	if err != nil {
		return nil, errorToFuseStatus(err)
	}
	var ents []fuse.DirEntry
	for _, name := range names {
		ents = append(ents, fuse.DirEntry{
			Mode: 0777,
			Name: name,
		})
	}
	return ents, fuse.OK
}

	// Symlinks.
func (f *fuseFS) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	return errorToFuseStatus(f.fs.SymLink(linkName, value))
}

func (f *fuseFS) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	link, err := f.fs.ReadLink(name)
	if err != nil {
		return "", errorToFuseStatus(err)
	}
	return link, fuse.OK
}

func (f *fuseFS) StatFs(name string) *fuse.StatfsOut {
	return nil
}
