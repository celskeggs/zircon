package filesystem

import (
	"os"
)

type Filesystem interface {
	Mkdir(path string) error
	Rename(source string, dest string) error
	Unlink(path string) error
	Rmdir(path string) error
	OpenRead(path string) (ReadSeekCloser, error)
	OpenWrite(path string) (ReadWriteSeekCloser, error)
	SymLink(source string, dest string) error
	Stat(path string) (os.FileInfo, error)
	ReadLink(path string) (string, error)
	Truncate(path string, length uint32) error
}
