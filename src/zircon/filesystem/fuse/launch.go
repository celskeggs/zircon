package fuse

import (
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"zircon/filesystem"
	"time"
)

func MountFuse(config filesystem.Configuration) error {
	fs, err := filesystem.NewFilesystemClient(config)
	if err != nil {
		return err
	}

	pathFs := pathfs.NewPathNodeFs(NewFuseFS(fs), &pathfs.PathNodeFsOptions{
		Debug: true,
	})
	server, _, err := nodefs.MountRoot(config.MountPoint, pathFs.Root(), &nodefs.Options{
		AttrTimeout: time.Second * 10,
		EntryTimeout: time.Second * 10,
		Debug: true,
	})
	if err != nil {
		return err
	}
	server.Serve()
	return nil
}
