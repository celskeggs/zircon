package client

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/util"
	"github.com/stretchr/testify/require"
)

// NOTE: Only simple tests are provided here. Everything else should either go into control/client_test.go or should be
// end-to-end tests that cover more than just this subsystem.

// Tests the ability for a single client to properly interact with a cluster, and
// perform a simple series of new, read, write, and delete operations, including
// correct error handling.
func TestSimpleClientReadWrite(t *testing.T) {
	config, _, teardown := PrepareNetworkedCluster(t)
	defer teardown()

	client, err := ConfigureNetworkedClient(config)
	require.NoError(t, err)
	defer client.Close()

	cn, err := client.New()
	assert.NoError(t, err)

	data, ver, err := client.Read(cn, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, apis.Version(0), ver)
	assert.Equal(t, []byte{0}, data)

	ver, err = client.Write(cn, 0, apis.AnyVersion, []byte("hello, world!"))
	assert.NoError(t, err)
	assert.True(t, ver > 0)

	data, ver2, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver, ver2)
	assert.Equal(t, "hello, world!", string(util.StripTrailingZeroes(data)))

	ver3, err := client.Write(cn, 7, ver2, []byte("home!"))
	assert.NoError(t, err)
	assert.True(t, ver3 > ver2)

	ver5, err := client.Write(cn, 7, ver2, []byte("earth..."))
	assert.Error(t, err)
	assert.Equal(t, ver3, ver5) // make sure it returns the correct new version after staleness failure

	data, ver4, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver3, ver4)
	assert.Equal(t, "hello, home!!", string(util.StripTrailingZeroes(data)))

	assert.Error(t, client.Delete(cn, ver2))

	data, ver6, err := client.Read(cn, 0, apis.MaxChunkSize)
	assert.NoError(t, err)
	assert.Equal(t, ver4, ver6)
	assert.Equal(t, "hello, home!!", string(util.StripTrailingZeroes(data)))

	assert.NoError(t, client.Delete(cn, ver6))

	_, _, err = client.Read(cn, 0, apis.MaxChunkSize)
	assert.Error(t, err)
}
