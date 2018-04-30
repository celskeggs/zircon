package control

import (
	testifyAssert "github.com/stretchr/testify/assert"
	"testing"
	"zircon/apis"
	"zircon/chunkserver/storage"
	"zircon/util"
)

// just for the chunk part, not for the version part
func TestChunkserverSingle(t *testing.T) {
	assert := testifyAssert.New(t)

	var chunkStorage storage.ChunkStorage = nil
	var cs apis.ChunkserverSingle = nil
	var teardown Teardown = nil

	test := func(name string, run func()) {
		t.Logf("subtest: %s", name)
		newStorage, err := storage.ConfigureMemoryStorage()
		assert.NoError(err)

		chunkStorage = newStorage

		cs, teardown, err = ExposeChunkserver(chunkStorage)
		assert.NoError(err)

		defer func() {
			if chunkStorage != nil {
				chunkStorage.Close()
				chunkStorage = nil

				teardown()
				cs = nil
			}
		}()
		run()
	}

	reopen := func() {
		// preserve storage, just recreate this interface.
		teardown()

		ncs, nteardown, err := ExposeChunkserver(chunkStorage)
		assert.NoError(err)

		cs, teardown = ncs, nteardown
	}

	test("empty by default", func() {
		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Empty(chunks)
	})

	test("can't read uncreated", func() {
		_, _, err := cs.Read(1, 0, 10, apis.AnyVersion)
		assert.Error(err)
		_, _, err = cs.Read(1, 0, 10, 1)
		assert.Error(err)
	})

	test("can't write uncreated", func() {
		assert.Error(cs.StartWrite(1, 0, []byte("test")))

		assert.Error(cs.CommitWrite(1, apis.CalculateCommitHash(0, []byte("test")), apis.AnyVersion, 1))

		assert.Error(cs.UpdateLatestVersion(1, apis.AnyVersion, 1))

		// ensure that chunks weren't created, despite errors
		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Empty(chunks)
	})

	test("can't delete uncreated", func() {
		assert.Error(cs.Delete(1, 1))
	})

	test("can't create new entry with version=0", func() {
		assert.Error(cs.Add(7, []byte("hello world"), 0))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Empty(chunks)
	})

	test("create new entry", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 3},
		}, chunks)

		data, version, err := cs.Read(7, 0, 256, apis.AnyVersion)
		assert.NoError(err)
		assert.Equal(apis.Version(3), version)
		assert.Equal(256, len(data))
		assert.Equal("hello world", string(util.StripTrailingZeroes(data)))

		data, version, err = cs.Read(7, 3, 5, 1)
		assert.NoError(err)
		assert.Equal(apis.Version(3), version)
		assert.Equal(5, len(data))
		assert.Equal("lo wo", string(data))

		data, version, err = cs.Read(7, 128, 512, 3)
		assert.NoError(err)
		assert.Equal(apis.Version(3), version)
		assert.Equal(512, len(data))
		assert.Empty(util.StripTrailingZeroes(data))

		data, version, err = cs.Read(7, 0, 256, 4)
		assert.Error(err)
		assert.Equal(apis.Version(3), version) // should still report latest version, even if it can't be provided
		assert.Empty(data)                     // no data on error
	})

	test("create new entry with durability", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		reopen()

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 3},
		}, chunks)

		data, version, err := cs.Read(7, 0, 256, apis.AnyVersion)
		assert.NoError(err)
		assert.Equal(apis.Version(3), version)
		assert.Equal(256, len(data))
		assert.Equal("hello world", string(util.StripTrailingZeroes(data)))
	})

	test("create new entry duplicate", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))
		assert.Error(cs.Add(7, []byte("goodbye world"), 4))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 3},
		}, chunks)
	})

	test("delete entry", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		assert.Error(cs.Delete(7, 2))
		assert.Error(cs.Delete(7, 4))
		assert.NoError(cs.Delete(7, 3))
		assert.Error(cs.Delete(7, 3))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Empty(chunks)

		data, version, err := cs.Read(7, 0, 256, apis.AnyVersion)
		assert.Error(err)
		assert.Equal(apis.Version(0), version) // version should be zero if none are available when the error occurs
		assert.Empty(data)                     // no data on failure
	})

	test("delete entry with durability", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		reopen()

		assert.Error(cs.Delete(7, 2))
		assert.Error(cs.Delete(7, 4))
		assert.NoError(cs.Delete(7, 3))
		assert.Error(cs.Delete(7, 3))

		reopen()

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Empty(chunks)

		data, version, err := cs.Read(7, 0, 256, apis.AnyVersion)
		assert.Error(err)
		assert.Equal(apis.Version(0), version) // version should be zero if none are available when the error occurs
		assert.Empty(data)                     // no data on failure
	})

	test("rewrite entry", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		// make sure the correct one is selected
		assert.NoError(cs.StartWrite(7, 0, []byte("Jell0")))
		assert.NoError(cs.StartWrite(7, 0, []byte("Hell")))
		assert.NoError(cs.StartWrite(7, 0, []byte("HELL0")))

		assert.Error(cs.CommitWrite(7, apis.CalculateCommitHash(0, []byte("Jell0")), 2, 3))
		assert.Error(cs.CommitWrite(7, apis.CalculateCommitHash(0, []byte("HELL0")), 4, 5))
		assert.NoError(cs.CommitWrite(7, apis.CalculateCommitHash(0, []byte("Hell")), 3, 4))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal(2, len(chunks))
		assert.Equal(apis.ChunkNum(7), chunks[0].Chunk)
		assert.Equal(apis.ChunkNum(7), chunks[1].Chunk)

		assert.True(chunks[0].Version == 3 || chunks[0].Version == 4)
		assert.True(chunks[1].Version == 3 || chunks[1].Version == 4)
		assert.True(chunks[0].Version != chunks[1].Version)

		for _, checkVer := range []apis.Version{apis.AnyVersion, 1, 2, 3} {
			data, ver, err := cs.Read(7, 0, 16, checkVer)
			assert.NoError(err)
			assert.Equal(apis.Version(3), ver)
			assert.Equal(16, len(data))
			assert.Equal("hello world", string(util.StripTrailingZeroes(data)))
		}

		// should *NOT* be exposed at all yet!
		data, ver, err := cs.Read(7, 0, 16, 4)
		assert.Error(err)
		assert.Equal(apis.Version(3), ver)
		assert.Empty(data)

		assert.Error(cs.UpdateLatestVersion(7, 2, 4))
		assert.Error(cs.UpdateLatestVersion(7, 3, 5))
		assert.NoError(cs.UpdateLatestVersion(7, 3, 4))

		chunks, err = cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 4},
		}, chunks)

		for _, checkVer := range []apis.Version{apis.AnyVersion, 1, 2, 3, 4} {
			data, ver, err := cs.Read(7, 0, 16, checkVer)
			assert.NoError(err)
			assert.Equal(apis.Version(4), ver)
			assert.Equal(16, len(data))
			assert.Equal("Hello world", string(util.StripTrailingZeroes(data)))
		}
	})

	test("rewrite entry with durability", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))

		reopen()

		assert.NoError(cs.StartWrite(7, 0, []byte("Hell")))

		// no reopen() here, because it's not guaranteed that partially started writes will get persisted.

		assert.NoError(cs.CommitWrite(7, apis.CalculateCommitHash(0, []byte("Hell")), 3, 4))

		reopen()

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal(2, len(chunks))
		assert.Equal(apis.ChunkNum(7), chunks[0].Chunk)
		assert.Equal(apis.ChunkNum(7), chunks[1].Chunk)

		assert.True(chunks[0].Version == 3 || chunks[0].Version == 4)
		assert.True(chunks[1].Version == 3 || chunks[1].Version == 4)
		assert.True(chunks[0].Version != chunks[1].Version)

		for _, checkVer := range []apis.Version{apis.AnyVersion, 1, 2, 3} {
			data, ver, err := cs.Read(7, 0, 16, checkVer)
			assert.NoError(err)
			assert.Equal(apis.Version(3), ver)
			assert.Equal(16, len(data))
			assert.Equal("hello world", string(util.StripTrailingZeroes(data)))
		}

		// should *NOT* be exposed at all yet!
		data, ver, err := cs.Read(7, 0, 16, 4)
		assert.Error(err)
		assert.Equal(apis.Version(3), ver)
		assert.Empty(data)

		reopen()

		assert.NoError(cs.UpdateLatestVersion(7, 3, 4))

		reopen()

		chunks, err = cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 4},
		}, chunks)

		for _, checkVer := range []apis.Version{apis.AnyVersion, 1, 2, 3, 4} {
			data, ver, err := cs.Read(7, 0, 16, checkVer)
			assert.NoError(err)
			assert.Equal(apis.Version(4), ver)
			assert.Equal(16, len(data))
			assert.Equal("Hello world", string(util.StripTrailingZeroes(data)))
		}
	})

	test("add data too large", func() {
		test := make([]byte, apis.MaxChunkSize+1)
		assert.Error(cs.Add(7, test, 3))
	})

	test("write data too large", func() {
		test := make([]byte, apis.MaxChunkSize)
		assert.NoError(cs.Add(7, test, 3))

		test = make([]byte, apis.MaxChunkSize+1)
		assert.Error(cs.StartWrite(7, 0, test))

		test = make([]byte, apis.MaxChunkSize)
		assert.Error(cs.StartWrite(7, 1, test))

		test = make([]byte, apis.MaxChunkSize-1)
		assert.NoError(cs.StartWrite(7, 1, test))
	})

	test("rollback new version", func() {
		assert.NoError(cs.Add(7, []byte("hello world"), 3))
		assert.NoError(cs.StartWrite(7, 0, []byte("Hell")))
		assert.NoError(cs.CommitWrite(7, apis.CalculateCommitHash(0, []byte("Hell")), 3, 4))
		assert.NoError(cs.Delete(7, 4))

		for _, checkVer := range []apis.Version{apis.AnyVersion, 1, 2, 3} {
			data, ver, err := cs.Read(7, 0, 16, checkVer)
			assert.NoError(err)
			assert.Equal(apis.Version(3), ver)
			assert.Equal(16, len(data))
			assert.Equal("hello world", string(util.StripTrailingZeroes(data)))
		}

		// should not exist at all!
		data, ver, err := cs.Read(7, 0, 16, 4)
		assert.Error(err)
		assert.Equal(apis.Version(3), ver)
		assert.Empty(data)

		assert.Error(cs.UpdateLatestVersion(7, 3, 4))

		chunks, err := cs.ListAllChunks()
		assert.NoError(err)
		assert.Equal([]struct {
			Chunk   apis.ChunkNum
			Version apis.Version
		}{
			{7, 3},
		}, chunks)
	})
}
