package chunkupdate

import (
	"testing"
	"zircon/apis"
	"zircon/rpc"
	"zircon/apis/mocks"
	"fmt"
	"math/rand"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/assert"
	"zircon/chunkserver"
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
	var allMocks []mock.Mock

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

	for id, fail := range replicaFails {
		expectSuccess = expectSuccess || !fail

		address := apis.ServerAddress(fmt.Sprintf("chunk-address-%d", id))

		replicaAddresses = append(replicaAddresses, address)

		chunkMock := &mocks.Chunkserver{}
		cache.Chunkservers[address] = chunkMock
		allMocks = append(allMocks, chunkMock.Mock)

		if fail {
			chunkMock.On("Read", chunk, offset, length, version).Return(nil, apis.Version(0), errors.New("sample failure for update_test"))
		} else {
			chunkMock.On("Read", chunk, offset, length, version).Return(data, realVersion, nil)
		}
	}

	if offset + length > apis.MaxChunkSize {
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

	for _, m := range allMocks {
		m.AssertExpectations(t)
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
	var allMocks []mock.Mock

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
		allMocks = append(allMocks, chunkMock.Mock)

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
	} else {
		assert.Error(t, err)
	}

	for _, m := range allMocks {
		m.AssertExpectations(t)
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

// test case covers: doesn't exist, n/a, n/a, n/a, n/a, no
// test case covers: exists, 0, 0, same, 0, yes
// test case covers: exists, 0, 0, same, >0, yes
// test case covers: exists, 0, >0, one off, >0, yes
// test case covers: exists, >0, >0, same, >0, yes
// test case covers: exists, >0, >0, one off, >0, yes
// test case covers: exists, >0, >0, further off, >0, yes
// test case covers: currently deleting, >0, 0, further off, >0, no

//   New partitions:
//     number of replicas: 0, 1, >1
//     number of replicas versus number of chunkservers: <, =, >
//     success: yes, no

// test case covers: 0, =, no
// test case covers: 0, <, no
// test case covers: 1, =, yes
// test case covers: 1, <, yes
// test case covers: 1, >, no
// test case covers: >1, =, yes

//   CommitWrite partitions:
//     chunk: exists, doesn't exist, currently deleting
//     number of replicas: 0, 1, >1
//     number of unavailable or prepareless replicas: 0, >0
//     version: matches, request newer, request older
//     success: yes, no

// test case covers: doesn't exist, n/a, n/a, n/a, no
// test case covers: exists, 0, 0, n/a, no
// test case covers: exists, 1, 0, matches, yes
// test case covers: exists, >1, >0, matches, no
// test case covers: exists, >1, 0, matches, yes
// test case covers: exists, 1, 0, request newer, no
// test case covers: exists, 1, 0, request older, no
// test case covers: currently deleting, 1, 0, matches, no

//   Delete
//     chunk: exists, doesn't exist, currently deleting
//     number of replicas: 0, 1, >1
//     fail to list: 0, >0
//     fail to delete: 0, >0
//     version: matches, request newer, request older
//     success: yes, no

// test case covers: doesn't exist, n/a, n/a, n/a, n/a, no
// test case covers: exists, 0, 0, 0, n/a, yes
// test case covers: exists, 1, 0, 0, matches, yes
// test case covers: exists, 1, 0, 0, request newer, no
// test case covers: exists, 1, 0, 0, request older, no
// test case covers: exists, >1, 0, 0, matches, yes
// test case covers: exists, >1, >0, 0, matches, no
// test case covers: exists, >1, 0, >0, matches, no
// test case covers: currently deleting, 1, 0, 0, matches, no
