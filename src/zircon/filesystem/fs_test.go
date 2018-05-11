package filesystem

import (
	"testing"
	"zircon/client"
	"zircon/filesystem/syncserver"
	"zircon/rpc"
	"github.com/stretchr/testify/require"
	"zircon/util"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
)

func ConstructFilesystemTestCluster(t *testing.T) (new func() Filesystem, teardown func()) {
	teardowns := &util.MultiTeardown{}
	clientConfig, newEtcd, teardown := client.PrepareNetworkedCluster(t)
	teardowns.Add(teardown)

	// TODO: move some of this setup into dedicated functions

	config := Configuration{
		ClientConfig: clientConfig,
	}

	for i := 0; i < 3; i++ {
		ssetcd := newEtcd()
		ssclient, err := client.ConfigureNetworkedClient(clientConfig)
		teardowns.Add(func() {
			ssclient.Close()
		})
		require.NoError(t, err)
		ss := syncserver.NewSyncServer(ssetcd, ssclient)
		end, address, err := rpc.PublishSyncServer(ss, "127.0.0.1:0")
		require.NoError(t, err)
		teardowns.Add(func() {
			end(true)
		})
		config.SyncServerAddresses = append(config.SyncServerAddresses, address)
	}

	return func() Filesystem {
		fs, err := NewFilesystemClient(config)
		require.NoError(t, err)
		return fs
	}, teardowns.Teardown
}

func TestSimpleOperations(t *testing.T) {
	newFS, teardown := ConstructFilesystemTestCluster(t)
	defer teardown()

	fs := newFS()

	assert.Error(t, fs.Mkdir("/tmp/test"))
	require.NoError(t, fs.Mkdir("/tmp"))
	require.NoError(t, fs.Mkdir("/tmp/test"))
	assert.Error(t, fs.Mkdir("/tmp/test"))

	// it's non-existent!
	_, err := fs.OpenRead("/tmp/test/log.txt")
	assert.Error(t, err)

	fileWrite, err := fs.OpenWrite("/tmp/test/log.txt", true,false)
	if assert.NoError(t, err) {
		n, err := fileWrite.Write([]byte("hello, world!\n"))
		assert.NoError(t, err)
		assert.Equal(t, 14, n)
		assert.NoError(t, fileWrite.Close())
	}

	fileRead, err := fs.OpenRead("/tmp/test/log.txt")
	if assert.NoError(t, err) {
		contents, err := ioutil.ReadAll(fileRead)
		assert.NoError(t, err)
		assert.Equal(t, "hello, world!\n", string(contents))
		assert.NoError(t, fileRead.Close())
	}

	contents, err := fs.ListDir("/")
	assert.NoError(t, err)
	assert.Equal(t, []string{"tmp"}, contents)

	contents, err = fs.ListDir("/tmp")
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, contents)

	contents, err = fs.ListDir("/tmp/")
	assert.NoError(t, err)
	assert.Equal(t, []string{"test"}, contents)

	contents, err = fs.ListDir("/tmp/test")
	assert.NoError(t, err)
	assert.Equal(t, []string{"log.txt"}, contents)
}
