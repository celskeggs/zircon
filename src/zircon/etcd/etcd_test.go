package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"zircon/apis"
)

// Just to make sure that our mechanism of launching etcd actually works.
func TestEtcdTesting(t *testing.T) {
	server, abort, err := LaunchTestingEtcdServer()
	if err != nil {
		t.Fatal(err)
	}
	defer abort()

	client, err := clientv3.NewFromURL(server)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Put(context.Background(), "hello-world", "hello-human")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(context.Background(), "hello-world")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "hello-human", string(resp.Kvs[0].Value))
}

func PrepareTwoClients(t *testing.T) (apis.EtcdInterface, apis.EtcdInterface, func()) {
	sub, teardown0 := PrepareSubscribeForTesting(t)
	iface1, teardown1 := sub("test-name")
	iface2, teardown2 := sub("test-name-2")

	return iface1, iface2, func() {
		teardown2()
		teardown1()
		teardown0()
	}
}

func TestGetName(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	assert.Equal(t, "test-name", string(iface1.GetName()))
	assert.Equal(t, "test-name-2", string(iface2.GetName()))
}

func TestGetUpdateAddress(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	// we do this for three different server types, so that we know that they're treated independently.
	for _, kind := range []apis.ServerType{apis.FRONTEND, apis.CHUNKSERVER, apis.METADATACACHE} {
		_, err := iface1.GetAddress(iface1.GetName(), kind)
		assert.Error(t, err)
		_, err = iface1.GetAddress(iface2.GetName(), kind)
		assert.Error(t, err)

		address1 := apis.ServerAddress(fmt.Sprintf("test-address-%v", kind))
		address2 := apis.ServerAddress(fmt.Sprintf("test-address-updated-%v", kind))

		assert.NoError(t, iface2.UpdateAddress(address1, kind))

		resp, err := iface1.GetAddress(iface2.GetName(), kind)
		assert.NoError(t, err)
		assert.Equal(t, apis.ServerAddress(address1), resp)
		resp, err = iface2.GetAddress(iface2.GetName(), kind)
		assert.NoError(t, err)
		assert.Equal(t, apis.ServerAddress(address1), resp)

		assert.NoError(t, iface2.UpdateAddress(address2, kind))

		resp, err = iface1.GetAddress(iface2.GetName(), kind)
		assert.NoError(t, err)
		assert.Equal(t, apis.ServerAddress(address2), resp)
		resp, err = iface2.GetAddress(iface2.GetName(), kind)
		assert.NoError(t, err)
		assert.Equal(t, apis.ServerAddress(address2), resp)
	}
}

func TestListServers(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	assert.NoError(t, iface1.UpdateAddress("test-addr-1", apis.CHUNKSERVER))
	assert.NoError(t, iface2.UpdateAddress("test-addr-2", apis.CHUNKSERVER))
	assert.NoError(t, iface2.UpdateAddress("test-addr-3", apis.FRONTEND))

	servers, err := iface1.ListServers(apis.METADATACACHE)
	assert.NoError(t, err)
	assert.Empty(t, servers)

	servers, err = iface1.ListServers(apis.CHUNKSERVER)
	assert.NoError(t, err)
	assert.Equal(t, []apis.ServerName{"test-name", "test-name-2"}, servers)

	servers, err = iface1.ListServers(apis.FRONTEND)
	assert.NoError(t, err)
	assert.Equal(t, []apis.ServerName{"test-name-2"}, servers)
}

// Tests claiming, disclaiming, and timeouts
func TestMetadataLeases(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	assert.Equal(t, TestingLeaseTimeout, iface1.GetMetadataLeaseTimeout())
	assert.Equal(t, TestingLeaseTimeout, iface2.GetMetadataLeaseTimeout())

	attemptClaims := func(server apis.EtcdInterface, id apis.MetadataID, expected apis.EtcdInterface) {
		owner, err := server.TryClaimingMetadata(id)
		assert.NoError(t, err)
		assert.Equal(t, expected.GetName(), owner)
	}

	attemptClaimsDual := func(first apis.EtcdInterface, id apis.MetadataID, expected apis.EtcdInterface) {
		attemptClaims(first, id, expected)
		if first == iface1 {
			attemptClaims(iface2, id, expected)
		} else if first == iface2 {
			attemptClaims(iface1, id, expected)
		} else {
			panic("test incorrectly written")
		}
	}

	assert.Error(t, iface1.RenewMetadataClaims())
	assert.Error(t, iface2.RenewMetadataClaims())

	_, err := iface1.TryClaimingMetadata(3)
	assert.Error(t, err)
	_, err = iface2.TryClaimingMetadata(3)
	assert.Error(t, err)

	assert.NoError(t, iface1.BeginMetadataLease())
	assert.NoError(t, iface2.BeginMetadataLease())

	assert.Error(t, iface1.BeginMetadataLease())
	assert.Error(t, iface2.BeginMetadataLease())

	assert.NoError(t, iface1.RenewMetadataClaims())
	assert.NoError(t, iface2.RenewMetadataClaims())

	attemptClaimsDual(iface2, 3, iface2)
	attemptClaimsDual(iface1, 5, iface1)

	assert.NoError(t, iface1.RenewMetadataClaims())
	assert.NoError(t, iface2.RenewMetadataClaims())

	assert.Error(t, iface1.DisclaimMetadata(3))
	attemptClaimsDual(iface1, 3, iface2)

	assert.NoError(t, iface2.DisclaimMetadata(3))
	attemptClaimsDual(iface1, 3, iface1)

	assert.NoError(t, iface2.RenewMetadataClaims())
	time.Sleep(TestingLeaseTimeout / 2)
	assert.NoError(t, iface2.RenewMetadataClaims())
	attemptClaims(iface2, 3, iface1)
	time.Sleep(TestingLeaseTimeout / 2)
	assert.NoError(t, iface2.RenewMetadataClaims())
	time.Sleep(TestingLeaseTimeout / 2)
	assert.NoError(t, iface2.RenewMetadataClaims())
	attemptClaims(iface2, 3, iface2)
	_, err = iface1.TryClaimingMetadata(77)
	assert.Error(t, err)
	owner, err := iface2.TryClaimingMetadata(77)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), owner)
	_, err = iface1.TryClaimingMetadata(3)
	assert.Error(t, err)
	assert.Error(t, iface1.RenewMetadataClaims())

	_, err = iface1.TryClaimingMetadata(6)
	assert.Error(t, err)
	assert.NoError(t, iface1.BeginMetadataLease())
	assert.NoError(t, iface1.RenewMetadataClaims())
	assert.NoError(t, iface2.RenewMetadataClaims())

	attemptClaims(iface1, 3, iface2)
	attemptClaimsDual(iface2, 6, iface2)
	attemptClaimsDual(iface1, 7, iface1)
}

func TestLeaseAnyMetametadata(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	assert.NoError(t, iface1.BeginMetadataLease())
	assert.NoError(t, iface2.BeginMetadataLease())

	owner, err := iface1.TryClaimingMetadata(1)
	assert.NoError(t, err)
	assert.Equal(t, iface1.GetName(), owner)
	assert.NoError(t, iface1.UpdateMetametadata(1, apis.MetadataEntry{}, apis.MetadataEntry{MostRecentVersion: 1}))

	owner, err = iface2.TryClaimingMetadata(4)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), owner)
	assert.NoError(t, iface2.UpdateMetametadata(4, apis.MetadataEntry{}, apis.MetadataEntry{MostRecentVersion: 4}))

	owner, err = iface2.TryClaimingMetadata(3)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), owner)
	assert.NoError(t, iface2.UpdateMetametadata(3, apis.MetadataEntry{}, apis.MetadataEntry{MostRecentVersion: 3}))
	assert.NoError(t, iface2.DisclaimMetadata(3))

	metadata, err := iface1.LeaseAnyMetametadata()
	assert.NoError(t, err)
	assert.Equal(t, apis.MetadataID(3), metadata)

	metadata, err = iface1.LeaseAnyMetametadata()
	assert.NoError(t, err)
	assert.Equal(t, apis.MetadataID(0), metadata)
}

func TestReadWriteMetadata(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	assert.Equal(t, TestingLeaseTimeout, iface1.GetMetadataLeaseTimeout())
	assert.Equal(t, TestingLeaseTimeout, iface2.GetMetadataLeaseTimeout())

	assert.NoError(t, iface1.BeginMetadataLease())
	assert.NoError(t, iface2.BeginMetadataLease())

	// fails because there's no claim
	_, err := iface1.GetMetametadata(3)
	assert.Error(t, err)

	sampleMetametadata := apis.MetadataEntry{
		LastConsumedVersion: 61,
		MostRecentVersion:   62,
		Replicas:            []apis.ServerID{5, 43, 1524},
	}

	// fails because no claim
	assert.Error(t, iface1.UpdateMetametadata(3, apis.MetadataEntry{}, sampleMetametadata))

	owner, err := iface1.TryClaimingMetadata(3)
	assert.NoError(t, err)
	assert.Equal(t, iface1.GetName(), owner)

	data, err := iface1.GetMetametadata(3)
	assert.NoError(t, err)
	assert.Equal(t, apis.Version(0), data.LastConsumedVersion)
	assert.Equal(t, apis.Version(0), data.MostRecentVersion)
	assert.Empty(t, data.Replicas)

	assert.NoError(t, iface1.UpdateMetametadata(3, apis.MetadataEntry{}, sampleMetametadata))
	data, err = iface1.GetMetametadata(3)
	assert.NoError(t, err)
	assert.Equal(t, sampleMetametadata, data)

	assert.NoError(t, iface1.RenewMetadataClaims())
	assert.NoError(t, iface2.RenewMetadataClaims())

	owner, err = iface2.TryClaimingMetadata(3)
	assert.NoError(t, err)
	assert.Equal(t, iface1.GetName(), owner)

	// fails because not claimed
	_, err = iface2.GetMetametadata(3)
	assert.Error(t, err)
}

func TestServerIDTracking(t *testing.T) {
	iface1, iface2, teardown := PrepareTwoClients(t)
	defer teardown()

	_, err := iface2.GetIDByName(iface2.GetName())
	assert.Error(t, err)
	_, err = iface1.GetIDByName(iface2.GetName())
	assert.Error(t, err)

	assert.NoError(t, iface2.UpdateAddress("test", apis.FRONTEND))

	sid, err := iface2.GetIDByName(iface2.GetName())
	assert.NoError(t, err)
	name, err := iface2.GetNameByID(sid)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), name)
	name, err = iface1.GetNameByID(sid)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), name)

	assert.NoError(t, iface2.UpdateAddress("test2", apis.FRONTEND))
	assert.NoError(t, iface1.UpdateAddress("test", apis.FRONTEND))

	sid2, err := iface2.GetIDByName(iface2.GetName())
	assert.NoError(t, err)
	assert.Equal(t, sid, sid2)
	sid3, err := iface2.GetIDByName(iface1.GetName())
	assert.NoError(t, err)
	assert.NotEqual(t, sid, sid3)

	name, err = iface2.GetNameByID(sid2)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), name)
	name, err = iface1.GetNameByID(sid2)
	assert.NoError(t, err)
	assert.Equal(t, iface2.GetName(), name)
	name, err = iface2.GetNameByID(sid3)
	assert.NoError(t, err)
	assert.Equal(t, iface1.GetName(), name)
	name, err = iface1.GetNameByID(sid3)
	assert.NoError(t, err)
	assert.Equal(t, iface1.GetName(), name)
}
