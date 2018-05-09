package chunkupdate

import (
	"testing"
	"zircon/apis"
	"zircon/rpc"
	"zircon/apis/mocks"
	"fmt"
	"math/rand"
	"errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
	"zircon/chunkserver"
	mocks2 "zircon/chunkupdate/mocks"
	"sort"
)

// Testing strategy split throughout file

//   PerformRead partitions:
//     offset+length = 0, 0<x<apis.MaxChunkSize, apis.MaxChunkSize, >apis.MaxChunkSize
//     replica # = 0, 1, >1
//     first chunkserver: fails, doesn't fail
//     # of failing chunkservers: 0, 1, 1<x<all, all
//     read successful: yes, no

func GenericTestPerformRead(t *testing.T, offset uint32, length uint32, replicaFails []bool) {
	cache := &rpc.MockCache{
		Chunkservers: map[apis.ServerAddress]apis.Chunkserver{},
	}
	var allMocks []*mock.Mock

	chunk := apis.ChunkNum(rand.Uint64())
	version := apis.Version(rand.Uint64())
	realVersion := apis.Version(rand.Uint64())
	if realVersion < version {
		realVersion, version = version, realVersion
	}
	data := make([]byte, length)
	for i := 0; i < int(length); i++ {
		data[i] = "fake"[i % 4]
	}

	// ** prepare mocked etcd responses and chunkservers **

	expectSuccess := false
	var replicaAddresses []apis.ServerAddress
	sizeFail := offset + length > apis.MaxChunkSize

	for id, fail := range replicaFails {
		expectSuccess = expectSuccess || !fail

		address := apis.ServerAddress(fmt.Sprintf("chunk-address-%d", id))

		replicaAddresses = append(replicaAddresses, address)

		chunkMock := &mocks.Chunkserver{}
		cache.Chunkservers[address] = chunkMock
		allMocks = append(allMocks, &chunkMock.Mock)

		if sizeFail {
			// don't expect anything
		} else if fail {
			chunkMock.On("Read", chunk, offset, length, version).Return(nil, apis.Version(0), errors.New("sample failure for update_test"))
		} else {
			chunkMock.On("Read", chunk, offset, length, version).Return(data, realVersion, nil)
		}
	}

	if sizeFail {
		expectSuccess = false
	}

	// now perform operation

	resultData, resultVersion, err := (&Reference{
		Replicas: replicaAddresses,
		Version: version,
		Chunk: chunk,
	}).PerformRead(cache, offset, length)

	if expectSuccess {
		assert.NoError(t, err)
		assert.Equal(t, data, resultData)
		assert.Equal(t, realVersion, resultVersion)
	} else {
		assert.Error(t, err)
	}

	// only confirm if we *know* that everything should have been called
	if (!expectSuccess || len(replicaFails) == 1) && !sizeFail {
		for _, m := range allMocks {
			m.AssertExpectations(t)
		}
	}
}

// test case covers: 0, 0, n/a, 0, no
func TestPerformRead_NoReplicas_Empty(t *testing.T) {
	GenericTestPerformRead(t, 2, 0, nil)
}
// test case covers: 0<x<apis.MaxChunkSize, 0, n/a, 0, no
func TestPerformRead_NoReplicas(t *testing.T) {
	GenericTestPerformRead(t, 3, 128, nil)
}
// test case covers: 0, 1, fails, 1, no
func TestPerformRead_OneReplica_Empty_Fail(t *testing.T) {
	GenericTestPerformRead(t, 4, 0, []bool { true })
}
// test case covers: 0<x<apis.MaxChunkSize, 1, fails, 1, no
func TestPerformRead_OneReplica_Fail(t *testing.T) {
	GenericTestPerformRead(t, 5, 128, []bool { true })
}
// test case covers: 0, 1, doesn't fail, 0, yes
func TestPerformRead_OneReplica_Pass_Empty(t *testing.T) {
	GenericTestPerformRead(t, 0, 0, []bool { false })
	GenericTestPerformRead(t, 6, 0, []bool { false })
}
// test case covers: apis.MaxChunkSize, 1, doesn't fail, 0, yes
func TestPerformRead_OneReplica_Pass_Max(t *testing.T) {
	GenericTestPerformRead(t, 0, apis.MaxChunkSize, []bool { false })
	GenericTestPerformRead(t, 7, apis.MaxChunkSize-7, []bool { false })
}
// test case covers: >apis.MaxChunkSize, 1, doesn't fail, 0, no
func TestPerformRead_OneReplica_Pass_OverFull(t *testing.T) {
	GenericTestPerformRead(t, 0, apis.MaxChunkSize+1, []bool { false })
	GenericTestPerformRead(t, 8, apis.MaxChunkSize-7, []bool { false })
}
// test case covers: 0<x<apis.MaxChunkSize, 1, doesn't fail, 0, yes
func TestPerformRead_OneReplica_Pass(t *testing.T) {
	GenericTestPerformRead(t, 0, 128, []bool { false })
	GenericTestPerformRead(t, 9, 128, []bool { false })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, fails, 1, yes
func TestPerformRead_ManyReplicas_PartialFailure(t *testing.T) {
	GenericTestPerformRead(t, 0, 512, []bool { true, false })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, fails, 1<x<all, yes
func TestPerformRead_ManyReplicas_PartialFailure_Large(t *testing.T) {
	GenericTestPerformRead(t, 0, 512, []bool { true, true, true, false, true })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, fails, all, no
func TestPerformRead_ManyReplicas_Fail(t *testing.T) {
	GenericTestPerformRead(t, 0, 512, []bool { true, true, true, true, true })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, doesn't fail, 1<x<all, yes
func TestPerformRead_ManyReplicas_PassFirst(t *testing.T) {
	GenericTestPerformRead(t, 0, 512, []bool { false, true, true, true, true })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, doesn't fail, 0, yes
func TestPerformRead_ManyReplicas_PassAll(t *testing.T) {
	GenericTestPerformRead(t, 0, 512, []bool { false, false, false })
}

//   PrepareWrite partitions:
//     offset+length = 0, 0<x<apis.MaxChunkSize, apis.MaxChunkSize, >apis.MaxChunkSize
//     replica # = 0, 1, >1
//     # of failing chunkservers: 0, 1, 1<x<all, all
//     write successful: yes, no

func GenericTestPrepareWrite(t *testing.T, offset uint32, length uint32, replicaFails []bool) {
	cache := &rpc.MockCache{
		Chunkservers: map[apis.ServerAddress]apis.Chunkserver{},
	}
	var allMocks []*mock.Mock

	chunk := apis.ChunkNum(rand.Uint64())
	data := make([]byte, length)
	for i := 0; i < int(length); i++ {
		data[i] = "fake"[i % 4]
	}

	expectedHash := apis.CalculateCommitHash(offset, data)

	// ** prepare mocked etcd responses and chunkservers **

	expectSuccess := true
	var replicaAddresses []apis.ServerAddress

	for id, fail := range replicaFails {
		expectSuccess = expectSuccess && !fail

		address := apis.ServerAddress(fmt.Sprintf("chunk-address-%d", id))

		replicaAddresses = append(replicaAddresses, address)

		chunkMock := &mocks.Chunkserver{}
		chunkChatter, err := chunkserver.WithChatter(chunkMock, cache)
		assert.NoError(t, err)
		cache.Chunkservers[address] = chunkChatter
		allMocks = append(allMocks, &chunkMock.Mock)

		if fail {
			chunkMock.On("StartWrite", chunk, offset, data).Return(errors.New("sample failure for update_test"))
		} else {
			chunkMock.On("StartWrite", chunk, offset, data).Return(nil)
		}
	}

	if offset + length > apis.MaxChunkSize || len(replicaFails) == 0 {
		expectSuccess = false
	}

	// now perform operation

	hash, err := (&Reference{
		Replicas: replicaAddresses,
		Version: 5,
		Chunk: chunk,
	}).PrepareWrite(cache, offset, data)

	if expectSuccess {
		assert.NoError(t, err)
		assert.Equal(t, expectedHash, hash)

		for _, m := range allMocks {
			m.AssertExpectations(t)
		}
	} else {
		assert.Error(t, err)
	}
}

//   PrepareWrite partitions:
//     offset+length = 0, 0<x<apis.MaxChunkSize, apis.MaxChunkSize, >apis.MaxChunkSize
//     replica # = 0, 1, >1
//     # of failing chunkservers: 0, 1, 1<x<all, all
//     write successful: yes, no

// test case covers: 0, 0, 0, no
func TestPrepareWrite_NoReplicas_Empty(t *testing.T) {
	GenericTestPrepareWrite(t, 1, 0, nil)
}
// test case covers: 0<x<apis.MaxChunkSize, 0, 0, no
func TestPrepareWrite_NoReplicas(t *testing.T) {
	GenericTestPrepareWrite(t, 2, 128, nil)
}
// test case covers: 0, 1, 0, yes
func TestPrepareWrite_OneReplica_Empty(t *testing.T) {
	GenericTestPrepareWrite(t, 3, 0, []bool{ false })
}
// test case covers: 0, 1, 1, no
func TestPrepareWrite_OneReplica_Fail_Empty(t *testing.T) {
	GenericTestPrepareWrite(t, 4, 0, []bool{ true })
}
// test case covers: 0, >1, 0, yes
func TestPrepareWrite_ManyReplicas_Empty(t *testing.T) {
	GenericTestPrepareWrite(t, 5, 0, []bool{ false, false, false })
}
// test case covers: 0, >1, 1<x<all, no
func TestPrepareWrite_ManyReplicas_PartialFail_Empty(t *testing.T) {
	GenericTestPrepareWrite(t, 6, 0, []bool{ false, true, false })
}
// test case covers: apis.MaxChunkSize, 1, 0, yes
func TestPrepareWrite_OneReplica_Max(t *testing.T) {
	GenericTestPrepareWrite(t, 0, apis.MaxChunkSize, []bool{ false })
	GenericTestPrepareWrite(t, 7, apis.MaxChunkSize - 7, []bool{ false })
}
// test case covers: >apis.MaxChunkSize, 1, 0, no
func TestPrepareWrite_OneReplica_OverMax(t *testing.T) {
	GenericTestPrepareWrite(t, 0, apis.MaxChunkSize + 1, []bool{ false })
	GenericTestPrepareWrite(t, 8, apis.MaxChunkSize - 7, []bool{ false })
}
// test case covers: 0<x<apis.MaxChunkSize, 1, 0, yes
func TestPrepareWrite_OneReplica(t *testing.T) {
	GenericTestPrepareWrite(t, 9, 128, []bool { false })
}
// test case covers: 0<x<apis.MaxChunkSize, 1, 1, no
func TestPrepareWrite_OneReplica_Fail(t *testing.T) {
	GenericTestPrepareWrite(t, 10, 128, []bool { true })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, 1, no  (note: tries many times to ensure consistency)
func TestPrepareWrite_ManyReplicas_ExactlyOneFail(t *testing.T) {
	for i := uint32(0); i < 50; i++ {
		for j := 0; j < 6; j++ {
			fails := make([]bool, 6)
			fails[j] = true
			GenericTestPrepareWrite(t, i, 128, fails)
		}
	}
}
// test case covers: 0<x<apis.MaxChunkSize, >1, all, no
func TestPrepareWrite_ManyReplicas_AllFail(t *testing.T) {
	GenericTestPrepareWrite(t, 11, 128, []bool { true, true, true, true, true })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, 1<x<all, no
func TestPrepareWrite_ManyReplicas_SomeFail(t *testing.T) {
	GenericTestPrepareWrite(t, 12, 128, []bool { false, true, true, false, true, false })
}
// test case covers: 0<x<apis.MaxChunkSize, >1, 0, yes
func TestPrepareWrite_ManyReplicas(t *testing.T) {
	GenericTestPrepareWrite(t, 13, 512, []bool { false, false, false, false, false, false })
}

//   ReadMeta partitions:
//     chunk: exists, doesn't exist, currently deleting
//     MRV: 0, >0
//     LCV: 0, >0
//     MRV <> LCV: same, one off, further off
//     # replicas: 0, >0
//     success: yes, no

func GenericTestReadMeta(t *testing.T, exists bool, mrv apis.Version, lcv apis.Version, replicas int) {
	cache := &rpc.MockCache{}

	etcdMock := &mocks.EtcdInterface{}
	metadataMock := &mocks2.UpdaterMetadata{}
	allMocks := []*mock.Mock{&etcdMock.Mock, &metadataMock.Mock}

	updater := NewUpdater(cache, etcdMock, metadataMock)
	chunk := apis.ChunkNum(rand.Uint64())
	var replicaAddresses []apis.ServerAddress
	var replicaIDs []apis.ServerID

	expectSuccess := exists && !(lcv < mrv)

	// prepare mock operations!

	for repN := 0; repN < replicas; repN++ {
		replicaID := apis.ServerID(rand.Uint32())
		name := apis.ServerName(fmt.Sprintf("replica-%d", repN))
		address := apis.ServerAddress(fmt.Sprintf("address-%d", rand.Uint64()))

		replicaIDs = append(replicaIDs, replicaID)
		replicaAddresses = append(replicaAddresses, address)

		if expectSuccess {
			etcdMock.On("GetNameByID", replicaID).Return(name, nil)
			etcdMock.On("GetAddress", name, apis.CHUNKSERVER).Return(address, nil)
		}
	}

	if exists {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{
			Replicas:            replicaIDs,
			MostRecentVersion:   mrv,
			LastConsumedVersion: lcv,
		}, nil)
	} else {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{}, errors.New("no such chunk"))
	}

	// perform operation!

	ref, err := updater.ReadMeta(chunk)
	if expectSuccess {
		// expect success!
		assert.NoError(t, err)
		assert.Equal(t, chunk, ref.Chunk)
		assert.Equal(t, mrv, ref.Version)
		if len(replicaAddresses) == 0 {
			assert.Empty(t, ref.Replicas)
		} else {
			sort.Slice(replicaAddresses, func(i, j int) bool {
				return replicaAddresses[i] < replicaAddresses[j]
			})
			sort.Slice(ref.Replicas, func(i, j int) bool {
				return ref.Replicas[i] < ref.Replicas[j]
			})
			assert.Equal(t, replicaAddresses, ref.Replicas)
		}
	} else {
		assert.Error(t, err)
	}

	for _, m := range allMocks {
		m.AssertExpectations(t)
	}
}

// test case covers: doesn't exist, n/a, n/a, n/a, n/a, no
func TestReadMeta_NonExistent(t *testing.T) {
	GenericTestReadMeta(t, false, 1, 1, 1)
}
// test case covers: exists, 0, 0, same, 0, yes
func TestReadMeta_JustCreated_NoReplicas(t *testing.T) {
	GenericTestReadMeta(t, true, 0, 0, 0)
}
// test case covers: exists, 0, 0, same, >0, yes
func TestReadMeta_JustCreated_SomeReplicas(t *testing.T) {
	GenericTestReadMeta(t, true, 0, 0, 5)
}
// test case covers: exists, 0, >0, one off, >0, yes
func TestReadMeta_NearNew(t *testing.T) {
	GenericTestReadMeta(t, true, 0, 1, 3)
}
// test case covers: exists, >0, >0, same, >0, yes
func TestReadMeta_Populated(t *testing.T) {
	GenericTestReadMeta(t, true, 55, 55, 5)
}
// test case covers: exists, >0, >0, one off, >0, yes
func TestReadMeta_DifferentVersions(t *testing.T) {
	GenericTestReadMeta(t, true, 88, 89, 4)
}
// test case covers: exists, >0, >0, further off, >0, yes
func TestReadMeta_FarOffVersions(t *testing.T) {
	GenericTestReadMeta(t, true, 44, 1514324, 8)
}
// test case covers: currently deleting, >0, 0, further off, >0, no
func TestReadMeta_CurrentlyDeleting(t *testing.T) {
	GenericTestReadMeta(t, true, 0xFFFFFFFFFFFFFFFF, 0, 3)
}

//   New partitions:
//     number of replicas: 0, 1, >1
//     number of replicas versus number of chunkservers: <, =, >
//     success: yes, no

func GenericTestNew(t *testing.T, replicas int, chunkservers int) {
	cache := &rpc.MockCache{}

	etcdMock := &mocks.EtcdInterface{}
	metadataMock := &mocks2.UpdaterMetadata{}

	updater := NewUpdater(cache, etcdMock, metadataMock)
	chunk := apis.ChunkNum(rand.Uint64())
	var chunkIDs []apis.ServerID
	var chunkNames []apis.ServerName

	expectSuccess := replicas != 0 && replicas <= chunkservers

	// prepare mock operations!

	for csI := 0; csI < chunkservers; csI++ {
		replicaID := apis.ServerID(rand.Uint32())
		name := apis.ServerName(fmt.Sprintf("chunkserver-%d", csI))

		chunkIDs = append(chunkIDs, replicaID)
		chunkNames = append(chunkNames, name)

		if replicas != 0 {
			etcdMock.On("GetIDByName", name).Return(replicaID, nil)
		}
	}

	if replicas != 0 {
		etcdMock.On("ListServers", apis.CHUNKSERVER).Return(chunkNames, nil)
	}

	if expectSuccess {
		metadataMock.On("NewEntry").Return(chunk, nil)
		metadataMock.On("UpdateEntry", chunk, apis.MetadataEntry{}, mock.MatchedBy(func(ent apis.MetadataEntry) bool {
			// first, make sure all IDs are unique
			found := map[apis.ServerID]bool{}
			for _, replica := range ent.Replicas {
				if found[replica] {
					return false
				}
				found[replica] = true
			}
			// then make sure that all IDs are valid
			for _, replica := range ent.Replicas {
				foundAny := false
				for _, compare := range chunkIDs {
					foundAny = foundAny || compare == replica
				}
				if !foundAny {
					return false
				}
			}
			// then check that the right number of IDs are present, and the right uninitialized versions
			return ent.LastConsumedVersion == 0 && ent.MostRecentVersion == 0 && len(ent.Replicas) == replicas
		})).Return(nil)
	}

	// perform operation!

	foundChunk, err := updater.New(replicas)
	if expectSuccess {
		// expect success!
		assert.NoError(t, err)
		assert.Equal(t, chunk, foundChunk)
	} else {
		assert.Error(t, err)
	}

	etcdMock.AssertExpectations(t)
	metadataMock.AssertExpectations(t)
}

// test case covers: 0, =, no
func TestNew_NoReplicas_AtAll(t *testing.T) {
	GenericTestNew(t, 0, 0)
}
// test case covers: 0, <, no
func TestNew_NoReplicas_Chosen(t *testing.T) {
	GenericTestNew(t, 0, 3)
}
// test case covers: 1, =, yes
func TestNew_OneOfOneReplica(t *testing.T) {
	GenericTestNew(t, 1, 1)
}
// test case covers: 1, <, yes
func TestNew_OneReplica(t *testing.T) {
	GenericTestNew(t, 1, 3)
}
// test case covers: >1, >, no
func TestNew_NotEnoughReplicas(t *testing.T) {
	GenericTestNew(t, 2, 1)
}
// test case covers: >1, =, yes
func TestNew_ExactlyEnoughReplicas(t *testing.T) {
	GenericTestNew(t, 7, 7)
}

//   CommitWrite partitions:
//     chunk: exists, doesn't exist, currently deleting
//     number of replicas: 0, 1, >1
//     number of unavailable or prepareless replicas: 0, >0
//     version: matches, request newer, request older
//     success: yes, no

func GenericTestCommitWrite(t *testing.T, exists bool, deleting bool, replicaFails []bool, versionRelative int) {
	cache := &rpc.MockCache{
		Chunkservers: map[apis.ServerAddress]apis.Chunkserver{},
	}

	etcdMock := &mocks.EtcdInterface{}
	metadataMock := &mocks2.UpdaterMetadata{}
	allMocks := []*mock.Mock{&etcdMock.Mock, &metadataMock.Mock}

	updater := NewUpdater(cache, etcdMock, metadataMock)
	chunk := apis.ChunkNum(rand.Uint64())
	version := apis.Version(rand.Uint32() + 100)
	lcv := version + 3

	expectedHash := apis.CommitHash("!! FAKE HASH !!")

	var chunkserverIDs []apis.ServerID
	var chunkserverAddresses []apis.ServerAddress

	expectSuccess := exists && !deleting && versionRelative == 0 && len(replicaFails) != 0

	// prepare mock operations!

	for csI, fail := range replicaFails {
		expectSuccess = expectSuccess && !fail
		replicaID := apis.ServerID(rand.Uint32())
		name := apis.ServerName(fmt.Sprintf("chunkserver-%d", csI))
		address := apis.ServerAddress(fmt.Sprintf("address-%d", rand.Uint64()))

		chunkserverIDs = append(chunkserverIDs, replicaID)
		chunkserverAddresses = append(chunkserverAddresses, address)

		chunkMock := &mocks.Chunkserver{}
		allMocks = append(allMocks, &chunkMock.Mock)

		cache.Chunkservers[address] = chunkMock

		etcdMock.On("GetNameByID", replicaID).Return(name, nil)
		etcdMock.On("GetAddress", name, apis.CHUNKSERVER).Return(address, nil)

		if fail {
			chunkMock.On("CommitWrite", chunk, expectedHash, version, lcv+1).Return(errors.New("sample error for update_test"))
		} else {
			chunkMock.On("CommitWrite", chunk, expectedHash, version, lcv+1).Return(nil)
			chunkMock.On("UpdateLatestVersion", chunk, version, lcv+1).Return(nil)
		}
	}

	if deleting {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{
			MostRecentVersion:   0xFFFFFFFFFFFFFFFF,
			LastConsumedVersion: 0,
			Replicas:            chunkserverIDs,
		}, nil)
	} else if exists {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{
			MostRecentVersion:   version + apis.Version(versionRelative),
			LastConsumedVersion: lcv,
			Replicas:            chunkserverIDs,
		}, nil)
		if versionRelative == 0 {
			metadataMock.On("UpdateEntry", chunk, apis.MetadataEntry{
				MostRecentVersion:   version,
				LastConsumedVersion: lcv,
				Replicas:            chunkserverIDs,
			}, apis.MetadataEntry{
				MostRecentVersion:   version,
				LastConsumedVersion: lcv + 1,
				Replicas:            chunkserverIDs,
			}).Return(nil)
			if expectSuccess {
				metadataMock.On("UpdateEntry", chunk, apis.MetadataEntry{
					MostRecentVersion:   version,
					LastConsumedVersion: lcv + 1,
					Replicas:            chunkserverIDs,
				}, apis.MetadataEntry{
					MostRecentVersion:   lcv + 1,
					LastConsumedVersion: lcv + 1,
					Replicas:            chunkserverIDs,
				}).Return(nil)
			}
		}
	} else {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{}, errors.New("sample error in update_test"))
	}

	result, err := updater.CommitWrite(chunk, version, expectedHash)
	if expectSuccess {
		assert.NoError(t, err)
		assert.Equal(t, lcv+1, result)
	} else {
		assert.Error(t, err)
	}
}

// test case covers: doesn't exist, n/a, n/a, n/a, no
func TestCommitWrite_NoExist(t *testing.T) {
	GenericTestCommitWrite(t, false, false, nil, 0)
}
// test case covers: exists, 0, 0, n/a, no
func TestCommitWrite_NoReplicas(t *testing.T) {
	GenericTestCommitWrite(t, true, false, nil, 0)
}
// test case covers: exists, 1, 0, matches, yes
func TestCommitWrite_OneReplica_Pass(t *testing.T) {
	GenericTestCommitWrite(t, true, false, []bool { false }, 0)
}
// test case covers: exists, >1, >0, matches, no
func TestCommitWrite_ManyReplicas_Fail(t *testing.T) {
	GenericTestCommitWrite(t, true, false, []bool { false, false, true, false }, 0)
}
// test case covers: exists, >1, 0, matches, yes
func TestCommitWrite_ManyReplicas_Pass(t *testing.T) {
	GenericTestCommitWrite(t, true, false, []bool { false, false, false, false }, 0)
}
// test case covers: exists, 1, 0, request newer, no
func TestCommitWrite_OneReplicas_TooNew(t *testing.T) {
	GenericTestCommitWrite(t, true, false, []bool { false, false, false, false }, 1)
}
// test case covers: exists, 1, 0, request older, no
func TestCommitWrite_OneReplicas_TooOld(t *testing.T) {
	GenericTestCommitWrite(t, true, false, []bool { false, false, false, false }, -1)
}
// test case covers: currently deleting, 1, 0, matches, no
func TestCommitWrite_CurrentlyDeleting(t *testing.T) {
	GenericTestCommitWrite(t, true, true, []bool { false }, 0)
}

//   Delete
//     chunk: exists, doesn't exist, currently deleting
//     number of replicas: 0, 1, >1
//     fail to list: 0, >0
//     fail to delete: 0, >0
//     version: matches, request newer, request older
//     success: yes, no

func GenericTestDelete(t *testing.T, exists bool, deleting bool, replicaFailsList []bool, failDelete bool, versionRelative int) {
	cache := &rpc.MockCache{
		Chunkservers: map[apis.ServerAddress]apis.Chunkserver{},
	}

	etcdMock := &mocks.EtcdInterface{}
	metadataMock := &mocks2.UpdaterMetadata{}
	allMocks := []*mock.Mock{&etcdMock.Mock, &metadataMock.Mock}

	updater := NewUpdater(cache, etcdMock, metadataMock)
	chunk := apis.ChunkNum(rand.Uint64())
	otherChunk := apis.ChunkNum(rand.Uint64())
	version := apis.Version(rand.Uint32() + 5)

	var chunkserverIDs []apis.ServerID
	var chunkserverAddresses []apis.ServerAddress

	expectSuccess := exists && !deleting && !failDelete && versionRelative == 0

	// prepare mock operations!

	for csI, fail := range replicaFailsList {
		expectSuccess = expectSuccess && !fail
		replicaID := apis.ServerID(rand.Uint32())
		name := apis.ServerName(fmt.Sprintf("chunkserver-%d", csI))
		address := apis.ServerAddress(fmt.Sprintf("address-%d", rand.Uint64()))

		chunkserverIDs = append(chunkserverIDs, replicaID)
		chunkserverAddresses = append(chunkserverAddresses, address)

		chunkMock := &mocks.Chunkserver{}
		allMocks = append(allMocks, &chunkMock.Mock)

		cache.Chunkservers[address] = chunkMock

		etcdMock.On("GetNameByID", replicaID).Return(name, nil)
		etcdMock.On("GetAddress", name, apis.CHUNKSERVER).Return(address, nil)

		if fail {
			chunkMock.On("ListAllChunks").Return(nil, errors.New("sample error for update_test"))
		} else {
			// afterwards
			if failDelete {
				chunkMock.On("ListAllChunks").Return([]struct {
					Chunk   apis.ChunkNum
					Version apis.Version
				}{
					{chunk, version + 1},
					{otherChunk, version},
					{otherChunk, 3},
					{otherChunk, version + 1},
				}, nil)
			} else {
				chunkMock.On("ListAllChunks").Return([]struct {
					Chunk   apis.ChunkNum
					Version apis.Version
				}{
					{otherChunk, version},
					{otherChunk, 3},
					{otherChunk, version + 1},
				}, nil)
			}
			// beforehand
			chunkMock.On("ListAllChunks").Return([]struct {
				Chunk apis.ChunkNum
				Version apis.Version
			}{
				{chunk, version},
				{chunk, version + 1},
				{otherChunk, version},
				{otherChunk, 3},
				{otherChunk, version + 1},
			}, nil)
			chunkMock.On("Delete", chunk, version).Return(nil)
			if failDelete {
				chunkMock.On("Delete", chunk, version + 1).Return(errors.New("sample deletion error"))
			} else {
				chunkMock.On("Delete", chunk, version + 1).Return(nil)
			}
		}
	}

	if deleting {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{
			MostRecentVersion:   0xFFFFFFFFFFFFFFFF,
			LastConsumedVersion: 0,
			Replicas:            chunkserverIDs,
		}, nil)
	} else if exists {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{
			MostRecentVersion:   version + apis.Version(versionRelative),
			LastConsumedVersion: version + apis.Version(versionRelative),
			Replicas:            chunkserverIDs,
		}, nil)
		if versionRelative == 0 {
			metadataMock.On("UpdateEntry", chunk, apis.MetadataEntry{
				MostRecentVersion:   version,
				LastConsumedVersion: version + apis.Version(versionRelative),
				Replicas:            chunkserverIDs,
			}, apis.MetadataEntry{
				MostRecentVersion:   0xFFFFFFFFFFFFFFFF,
				LastConsumedVersion: 0,
				Replicas:            chunkserverIDs,
			}).Return(nil)
			if expectSuccess {
				metadataMock.On("DeleteEntry", chunk, apis.MetadataEntry{
					MostRecentVersion:   0xFFFFFFFFFFFFFFFF,
					LastConsumedVersion: 0,
					Replicas:            chunkserverIDs,
				}).Return(nil)
			}
		}
	} else {
		metadataMock.On("ReadEntry", chunk).Return(apis.MetadataEntry{}, errors.New("sample error in update_test"))
	}

	err := updater.Delete(chunk, version)
	if expectSuccess {
		assert.NoError(t, err)
	} else {
		assert.Error(t, err)
	}
}

// test case covers: doesn't exist, n/a, n/a, n/a, n/a, no
func TestDelete_NoExist(t *testing.T) {
	GenericTestDelete(t, false, false, nil, false, 0)
}
// test case covers: exists, 0, 0, 0, n/a, yes
func TestDelete_NoReplicas(t *testing.T) {
	GenericTestDelete(t, true, false, nil, false, 0)
}
// test case covers: exists, 1, 0, 0, matches, yes
func TestDelete_OneReplica(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false }, false, 0)
}
// test case covers: exists, 1, 0, 0, request newer, no
func TestDelete_OneReplica_Newer(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false }, false, 1)
}
// test case covers: exists, 1, 0, 0, request older, no
func TestDelete_OneReplica_Older(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false }, false, -1)
}
// test case covers: exists, >1, 0, 0, matches, yes
func TestDelete_ManyReplicas(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false, false, false }, false, 0)
}
// test case covers: exists, >1, >0, 0, matches, no
func TestDelete_ManyReplicas_FailList(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false, true, false }, false, 0)
}
// test case covers: exists, 1, 0, >0, matches, no
func TestDelete_OneReplica_FailDelete(t *testing.T) {
	GenericTestDelete(t, true, false, []bool { false }, true, 0)
}
// test case covers: currently deleting, 1, 0, 0, matches, no
func TestDelete_CurrentlyDeleting(t *testing.T) {
	GenericTestDelete(t, true, true, []bool { false }, false, 0)
}
