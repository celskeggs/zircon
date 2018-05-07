package test

import (
	testifyAssert "github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"zircon/apis"
	"zircon/chunkserver/storage"
	"zircon/util"
)

func sortChunkNums(chunks []apis.ChunkNum) {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i] < chunks[j]
	})
}

// just for the chunk part, not for the version part
func TestChunkStorage(openStorage func() storage.ChunkStorage, closeStorage func(storage.ChunkStorage),
	resetStorage func(), t *testing.T) {
	assert := testifyAssert.New(t)

	var s storage.ChunkStorage = nil

	test := func(name string, run func()) {
		t.Logf("subtest: %s", name)
		resetStorage()
		s = openStorage()
		defer func() {
			if s != nil {
				closeStorage(s)
				s = nil
			}
		}()
		run()
	}

	reopen := func() {
		closeStorage(s)
		// no reset
		s = openStorage()
	}

	test("empty by default", func() {
		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		assert.Empty(chunks)
	})

	test("no versions", func() {
		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Empty(versions)
	})

	test("cannot read nonexistent version", func() {
		_, err := s.ReadVersion(71, 1)
		assert.Error(err)
	})

	test("cannot delete nonexistent version", func() {
		err := s.DeleteVersion(71, 1)
		assert.Error(err)
	})

	test("write single chunk", func() {
		err := s.WriteVersion(71, 3, []byte("hello, world!\000\000\000"))
		assert.NoError(err)

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{3}, versions)

		data, err := s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("hello, world!"), util.StripTrailingZeroes(data))
	})

	test("write single chunk corrolaries", func() {
		err := s.WriteVersion(71, 3, []byte("hello, world!\000\000\000"))
		assert.NoError(err)

		versions, err := s.ListVersions(72)
		assert.NoError(err)
		assert.Empty(versions)

		_, err = s.ReadVersion(71, 2)
		assert.Error(err)

		_, err = s.ReadVersion(71, 4)
		assert.Error(err)

		_, err = s.ReadVersion(72, 3)
		assert.Error(err)
	})

	test("write single chunk durability", func() {
		err := s.WriteVersion(71, 3, []byte("hello, world!\000\000\000"))
		assert.NoError(err)

		reopen()

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{3}, versions)

		data, err := s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("hello, world!"), util.StripTrailingZeroes(data))
	})

	test("update single chunk durability", func() {
		assert.NoError(s.WriteVersion(71, 3, []byte("hello, world!\000\000\000")))

		reopen()

		assert.NoError(s.WriteVersion(71, 4, []byte("goodbye, world!\000\000\000")))

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{3, 4}, versions)

		data, err := s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("hello, world!"), util.StripTrailingZeroes(data))
		data, err = s.ReadVersion(71, 4)
		assert.NoError(err)
		assert.Equal([]byte("goodbye, world!"), util.StripTrailingZeroes(data))
	})

	test("prevent rewriting versions", func() {
		err := s.WriteVersion(71, 3, []byte("hello, world!\000\000\000"))
		assert.NoError(err)

		err = s.WriteVersion(71, 3, []byte("goodbye, world!\000\000\000"))
		assert.Error(err)

		data, err := s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("hello, world!"), util.StripTrailingZeroes(data))
	})

	test("write multiple chunks and versions", func() {
		assert.NoError(s.WriteVersion(71, 3, []byte("71-3")))
		assert.NoError(s.WriteVersion(72, 1, []byte("72-1")))
		assert.NoError(s.WriteVersion(71, 2, []byte("71-2")))

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		sortChunkNums(chunks)
		assert.Equal([]apis.ChunkNum{71, 72}, chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{2, 3}, versions)

		versions, err = s.ListVersions(72)
		assert.NoError(err)
		assert.Equal([]apis.Version{1}, versions)

		data, err := s.ReadVersion(71, 2)
		assert.NoError(err)
		assert.Equal([]byte("71-2"), util.StripTrailingZeroes(data))

		data, err = s.ReadVersion(72, 1)
		assert.NoError(err)
		assert.Equal([]byte("72-1"), util.StripTrailingZeroes(data))

		data, err = s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("71-3"), util.StripTrailingZeroes(data))
	})

	test("write multiple chunks and versions with durability", func() {
		assert.NoError(s.WriteVersion(71, 3, []byte("71-3")))
		assert.NoError(s.WriteVersion(72, 1, []byte("72-1")))
		assert.NoError(s.WriteVersion(71, 2, []byte("71-2")))

		reopen()

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		sortChunkNums(chunks)
		assert.Equal([]apis.ChunkNum{71, 72}, chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{2, 3}, versions)

		versions, err = s.ListVersions(72)
		assert.NoError(err)
		assert.Equal([]apis.Version{1}, versions)

		data, err := s.ReadVersion(71, 2)
		assert.NoError(err)
		assert.Equal([]byte("71-2"), util.StripTrailingZeroes(data))

		data, err = s.ReadVersion(72, 1)
		assert.NoError(err)
		assert.Equal([]byte("72-1"), util.StripTrailingZeroes(data))

		data, err = s.ReadVersion(71, 3)
		assert.NoError(err)
		assert.Equal([]byte("71-3"), util.StripTrailingZeroes(data))
	})

	test("write maximum length chunk of zeroes", func() {
		data := make([]byte, apis.MaxChunkSize)
		assert.NoError(s.WriteVersion(70, 100, data))

		data, err := s.ReadVersion(70, 100)
		assert.NoError(err)
		assert.Empty(util.StripTrailingZeroes(data))
	})

	test("write maximum length chunk with nonzero element", func() {
		write := make([]byte, apis.MaxChunkSize)
		write[len(write)-1] = 152
		assert.NoError(s.WriteVersion(70, 100, write))

		data, err := s.ReadVersion(70, 100)
		assert.NoError(err)
		assert.Equal(apis.MaxChunkSize, len(data))
		assert.Equal(uint8(152), data[len(data)-1])
		data[len(data)-1] = 0
		assert.Equal(uint8(152), write[len(write)-1])
		assert.Empty(util.StripTrailingZeroes(data))
	})

	test("write too large chunk", func() {
		data := make([]byte, apis.MaxChunkSize+1)
		assert.Error(s.WriteVersion(70, 100, data))

		versions, err := s.ListVersions(70)
		assert.NoError(err)
		assert.Empty(versions)
	})

	test("write minimum length chunk", func() {
		assert.NoError(s.WriteVersion(70, 100, []byte{}))

		data, err := s.ReadVersion(70, 100)
		assert.NoError(err)
		assert.Equal([]byte{}, util.StripTrailingZeroes(data))
	})

	test("delete subset of versions", func() {
		assert.NoError(s.WriteVersion(71, 3, []byte("71-3")))
		assert.NoError(s.WriteVersion(71, 1, []byte("71-1")))
		assert.NoError(s.WriteVersion(71, 2, []byte("71-2")))

		assert.NoError(s.DeleteVersion(71, 2))

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{1, 3}, versions)

		assert.Error(s.DeleteVersion(71, 2))
	})

	test("delete subset of versions with durability", func() {
		assert.NoError(s.WriteVersion(71, 3, []byte("71-3")))
		assert.NoError(s.WriteVersion(71, 1, []byte("71-1")))
		assert.NoError(s.WriteVersion(71, 2, []byte("71-2")))

		reopen()

		assert.NoError(s.DeleteVersion(71, 2))

		reopen()

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Equal([]apis.Version{1, 3}, versions)

		assert.Error(s.DeleteVersion(71, 2))
	})

	test("delete all versions", func() {
		assert.NoError(s.WriteVersion(71, 1, []byte("71-1")))
		assert.NoError(s.WriteVersion(71, 2, []byte("71-2")))

		assert.NoError(s.DeleteVersion(71, 1))
		assert.NoError(s.DeleteVersion(71, 2))

		chunks, err := s.ListChunksWithData()
		assert.NoError(err)
		assert.Empty(chunks)

		versions, err := s.ListVersions(71)
		assert.NoError(err)
		assert.Empty(versions)
	})
}
