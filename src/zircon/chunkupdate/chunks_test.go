package chunkupdate

import (
	"testing"
	"zircon/etcd"
	"zircon/apis"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
)

func TestListChunkservers(t *testing.T) {
	etcds, teardown := etcd.PrepareSubscribeForTesting(t)
	defer teardown()

	for i := 0; i < 5; i++ {
		etcdn, teardown2 := etcds(apis.ServerName(fmt.Sprintf("chunkserver-%d", i)))
		defer teardown2()
		if i != 3 {
			assert.NoError(t, etcdn.UpdateAddress(apis.ServerAddress(fmt.Sprintf("testaddress-%d", i)), apis.CHUNKSERVER))
		}
		if i == 2 {
			assert.NoError(t, etcdn.UpdateAddress("testaddress", apis.FRONTEND))
			assert.NoError(t, etcdn.UpdateAddress("testaddress", apis.METADATACACHE))
		}
	}

	etcdn, teardown3 := etcds("test")
	defer teardown3()
	ids, err := ListChunkservers(etcdn)
	assert.NoError(t, err)

	assert.Equal(t, 4, len(ids))
	var names []string
	for _, id := range ids {
		name, err := etcdn.GetNameByID(id)
		assert.NoError(t, err)
		names = append(names, string(name))
	}
	sort.Strings(names)
	assert.Equal(t, []string {
		"chunkserver-0",
		"chunkserver-1",
		"chunkserver-2",
		"chunkserver-4",
	}, names)
}

func TestAddressForChunkserver(t *testing.T) {
	etcds, teardown := etcd.PrepareSubscribeForTesting(t)
	defer teardown()

	ids := [5]apis.ServerID{}

	for i := 0; i < len(ids); i++ {
		name := apis.ServerName(fmt.Sprintf("chunkserver-%d", i))
		address := apis.ServerAddress(fmt.Sprintf("testaddress-%d", i))
		etcdn, teardown2 := etcds(name)
		defer teardown2()
		assert.NoError(t, etcdn.UpdateAddress(address, apis.CHUNKSERVER))
		id, err := etcdn.GetIDByName(name)
		assert.NoError(t, err)
		ids[i] = id
	}

	etcdn, teardown3 := etcds("query")
	defer teardown3()

	for i, id := range ids {
		expected := apis.ServerAddress(fmt.Sprintf("testaddress-%d", i))
		address, err := AddressForChunkserver(etcdn, id)
		assert.NoError(t, err)
		assert.Equal(t, expected, address)
	}
}
