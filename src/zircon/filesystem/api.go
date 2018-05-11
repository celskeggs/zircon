package filesystem

import (
	"os"
)

type Filesystem interface {
	Mkdir(path string) error
	Rename(source string, dest string) error
	Unlink(path string) error
	Rmdir(path string) error
	OpenRead(path string) (ReadOnlyFile, error)
	// Note: this does *NOT* truncate by default!
	OpenWrite(path string, create bool, exclusive bool) (WritableFile, error)
	SymLink(source string, dest string) error
	Stat(path string) (os.FileInfo, error)
	ReadLink(path string) (string, error)
	Truncate(path string, length uint32) error
	ListDir(path string) ([]string, error)

	GetTraverser() (*Traverser, error)
}
