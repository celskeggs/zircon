package test

import (
	"zircon/chunkserver/storage"
	"testing"
	testifyAssert "github.com/stretchr/testify/assert"
	"zircon/apis"
)

// just for the chunk part, not for the version part
func TestVersionStorage(openStorage func() storage.ChunkStorage, closeStorage func(storage.ChunkStorage),
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
		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Empty(chunks)
	})

	test("cannot get nonexistent latest", func() {
		_, err := s.GetLatestVersion(71)
		assert.Error(err)
	})

	test("cannot delete nonexistent version", func() {
		err := s.DeleteLatestVersion(71)
		assert.Error(err)
	})

	test("write single version", func() {
		assert.NoError(s.SetLatestVersion(71, 3))

		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		data, err := s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(3), data)
	})

	test("write single version corrolaries", func() {
		assert.NoError(s.SetLatestVersion(71, 3))

		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		_, err = s.GetLatestVersion(72)
		assert.Error(err)

		_, err = s.GetLatestVersion(70)
		assert.Error(err)
	})

	test("write single version durability", func() {
		assert.NoError(s.SetLatestVersion(71, 3))

		reopen()

		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{71}, chunks)

		data, err := s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(3), data)
	})

	test("updating versions", func() {
		assert.NoError(s.SetLatestVersion(71, 1))
		assert.NoError(s.SetLatestVersion(71, 3))

		data, err := s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(3), data)

		assert.NoError(s.SetLatestVersion(72, 6))
		assert.NoError(s.SetLatestVersion(71, 2))

		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		sortChunkNums(chunks)
		assert.Equal([]apis.ChunkNum{71, 72}, chunks)

		data, err = s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(2), data)

		data, err = s.GetLatestVersion(72)
		assert.NoError(err)
		assert.Equal(apis.Version(6), data)
	})

	test("updating versions with durability", func() {
		assert.NoError(s.SetLatestVersion(71, 1))
		assert.NoError(s.SetLatestVersion(71, 3))

		reopen()

		data, err := s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(3), data)

		assert.NoError(s.SetLatestVersion(72, 6))
		assert.NoError(s.SetLatestVersion(71, 2))

		chunks, err := s.ListChunksWithLatest()
		assert.NoError(err)
		sortChunkNums(chunks)
		assert.Equal([]apis.ChunkNum{71, 72}, chunks)

		data, err = s.GetLatestVersion(71)
		assert.NoError(err)
		assert.Equal(apis.Version(2), data)

		data, err = s.GetLatestVersion(72)
		assert.NoError(err)
		assert.Equal(apis.Version(6), data)
	})

	test("delete subset of versions", func() {
		assert.NoError(s.SetLatestVersion(71, 2))
		assert.NoError(s.SetLatestVersion(72, 6))

		assert.NoError(s.DeleteLatestVersion(71))

		versions, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{72}, versions)

		assert.Error(s.DeleteLatestVersion(71))
	})

	test("delete subset of versions with durabilitiy", func() {
		assert.NoError(s.SetLatestVersion(71, 2))
		assert.NoError(s.SetLatestVersion(72, 6))

		reopen()

		assert.NoError(s.DeleteLatestVersion(71))

		reopen()

		versions, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Equal([]apis.ChunkNum{72}, versions)

		assert.Error(s.DeleteLatestVersion(71))
	})

	test("delete all versions", func() {
		assert.NoError(s.SetLatestVersion(71, 2))
		assert.NoError(s.SetLatestVersion(72, 6))

		assert.NoError(s.DeleteLatestVersion(71))
		assert.NoError(s.DeleteLatestVersion(72))

		versions, err := s.ListChunksWithLatest()
		assert.NoError(err)
		assert.Empty(versions)

		_, err = s.GetLatestVersion(71)
		assert.Error(err)
	})
}
