package etcd

import (
	"testing"
	"zircon/apis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

func prepareSingleEtcdClient(t *testing.T) (apis.EtcdInterface, func()) {
	etcds, teardown1 := PrepareSubscribeForTesting(t)
	etcdn, teardown2 := etcds("client0")
	return etcdn, func() {
		teardown2()
		teardown1()
	}
}

func TestSyncServer_SingleSync_ReadOnly(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	syncid, err := etcd.StartSync(1)
	require.NoError(t, err)

	write, err := etcd.ConfirmSync(syncid)
	assert.NoError(t, err)
	assert.False(t, write)

	assert.NoError(t, etcd.ReleaseSync(syncid))

	_, err = etcd.ConfirmSync(syncid)
	assert.Error(t, err)
}

func TestSyncServer_SingleSync_RW(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	syncid, err := etcd.StartSync(1)
	require.NoError(t, err)

	write, err := etcd.ConfirmSync(syncid)
	assert.NoError(t, err)
	assert.False(t, write)

	writer, err := etcd.UpgradeSync(syncid)
	if assert.NoError(t, err) {
		write, err = etcd.ConfirmSync(writer)
		assert.NoError(t, err)
		assert.True(t, write)

		write, err = etcd.ConfirmSync(syncid)
		assert.NoError(t, err)
		assert.False(t, write)

		assert.NoError(t, etcd.ReleaseSync(writer))

		_, err = etcd.ConfirmSync(writer)
		assert.Error(t, err)
	}

	write, err = etcd.ConfirmSync(syncid)
	assert.NoError(t, err)
	assert.False(t, write)

	assert.NoError(t, etcd.ReleaseSync(syncid))

	_, err = etcd.ConfirmSync(syncid)
	assert.Error(t, err)
}

func TestSyncServer_BlockingUpgrade(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	sy1, err := etcd.StartSync(1)
	require.NoError(t, err)

	sy2, err := etcd.StartSync(1)
	require.NoError(t, err)
	assert.NotEqual(t, sy1, sy2)

	var ipt time.Time
	done := make(chan bool)

	go func() {
		ok := false
		defer func() {
			done <- ok
		}()
		syu2, err := etcd.UpgradeSync(sy2)
		assert.NoError(t, err)
		ipt = time.Now()

		assert.NoError(t, etcd.ReleaseSync(syu2))
		ok = true
	}()

	time.Sleep(time.Millisecond * 50)

	beginRelease := time.Now()
	err = etcd.ReleaseSync(sy1)
	isok := <-done
	endRelease := time.Now()
	assert.NoError(t, err)
	assert.True(t, isok)

	assert.True(t, beginRelease.Before(ipt))
	assert.True(t, endRelease.After(ipt))
	assert.True(t, endRelease.Sub(beginRelease) < time.Millisecond * 30, "took too long: %v", endRelease.Sub(beginRelease))

	assert.NoError(t, etcd.ReleaseSync(sy2))
}

func TestSyncServer_BlockingAcquire(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	sy1, err := etcd.StartSync(1)
	require.NoError(t, err)

	syu1, err := etcd.UpgradeSync(sy1)
	require.NoError(t, err)

	var ipt time.Time
	done := make(chan bool)

	go func() {
		ok := false
		defer func() {
			done <- ok
		}()
		sy2, err := etcd.StartSync(1)
		ipt = time.Now()
		assert.NoError(t, err)

		assert.NoError(t, etcd.ReleaseSync(sy2))
		ok = true
	}()

	assert.NoError(t, etcd.ReleaseSync(sy1))

	time.Sleep(time.Millisecond * 50)

	beginRelease := time.Now()
	err = etcd.ReleaseSync(syu1)
	isok := <-done
	endRelease := time.Now()
	assert.NoError(t, err)
	assert.True(t, isok)

	assert.True(t, beginRelease.Before(ipt))
	assert.True(t, endRelease.After(ipt))
	assert.True(t, endRelease.Sub(beginRelease) < time.Millisecond * 30, "took too long: %v", endRelease.Sub(beginRelease))
}

func TestSyncServer_NonConflicting(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	sy1, err := etcd.StartSync(1)
	require.NoError(t, err)

	_, err = etcd.UpgradeSync(sy1)
	require.NoError(t, err)

	sy2, err := etcd.StartSync(2)
	require.NoError(t, err)

	_, err = etcd.UpgradeSync(sy2)
	require.NoError(t, err)

	// if we got here: ah, good, no contention!
}

func TestSyncServer_DualEscalateFail(t *testing.T) {
	etcd, teardown := prepareSingleEtcdClient(t)
	defer teardown()

	sy1, err := etcd.StartSync(1)
	require.NoError(t, err)

	sy2, err := etcd.StartSync(1)
	require.NoError(t, err)

	var ipt time.Time

	done := make(chan bool)

	go func() {
		ok := false
		defer func() {
			done <- ok
		}()
		_, err = etcd.UpgradeSync(sy1)
		ipt = time.Now()
		assert.NoError(t, err)
		ok = true
	}()

	_, err = etcd.UpgradeSync(sy2)
	assert.Error(t, err) // because one of the upgrades should be blocked for the sake of not deadlocking

	time.Sleep(time.Millisecond * 100)

	beginRelease := time.Now()
	err = etcd.ReleaseSync(sy2)
	isok := <-done
	endRelease := time.Now()
	assert.NoError(t, err)
	assert.True(t, isok)

	assert.True(t, beginRelease.Before(ipt), "relative: %v", ipt.Sub(beginRelease))
	assert.True(t, endRelease.After(ipt))
	assert.True(t, endRelease.Sub(beginRelease) < time.Millisecond * 60, "took too long: %v", endRelease.Sub(beginRelease))
}
