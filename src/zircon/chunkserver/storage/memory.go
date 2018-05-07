package storage

import (
	"fmt"
	"sort"
	"zircon/apis"
)

type MemoryStorage struct {
	isClosed bool
	chunks   map[apis.ChunkNum]map[apis.Version][]byte
	latest   map[apis.ChunkNum]apis.Version
}

// Creates an in-memory-only location to store data, and construct an interface by which a chunkserver can store chunks
func ConfigureMemoryStorage() (ChunkStorage, error) {
	return &MemoryStorage{
		chunks: map[apis.ChunkNum]map[apis.Version][]byte{},
		latest: map[apis.ChunkNum]apis.Version{},
	}, nil
}

// returns semi-fake storage usage stats for testing
func (m *MemoryStorage) StatsForTesting() int {
	if m.isClosed {
		panic("attempt to use closed MemoryStorage")
	}
	chunkCount := 0
	for _, v := range m.chunks {
		chunkCount += len(v)
	}
	entryCount := len(m.chunks) + len(m.latest) + chunkCount
	// let's approximate 32 bytes per hash table entry
	// and 8 MB per chunk of data
	return entryCount*32 + chunkCount*int(apis.MaxChunkSize)
}

func (m *MemoryStorage) assertOpen() {
	if m.isClosed {
		panic("attempt to use closed MemoryStorage")
	}
}

func (m *MemoryStorage) ListChunksWithData() ([]apis.ChunkNum, error) {
	m.assertOpen()
	result := make([]apis.ChunkNum, 0, len(m.chunks))
	for k, v := range m.chunks {
		if len(v) > 0 {
			result = append(result, k)
		}
	}
	return result, nil
}

func (m *MemoryStorage) ListVersions(chunk apis.ChunkNum) ([]apis.Version, error) {
	m.assertOpen()
	versionMap := m.chunks[chunk]
	if versionMap == nil {
		return nil, nil
	}
	result := make([]apis.Version, 0, len(versionMap))
	for k, _ := range versionMap {
		result = append(result, k)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, nil
}

func (m *MemoryStorage) ReadVersion(chunk apis.ChunkNum, version apis.Version) ([]byte, error) {
	m.assertOpen()
	if versionMap := m.chunks[chunk]; versionMap != nil {
		if data, found := versionMap[version]; found {
			ndata := make([]byte, len(data))
			copy(ndata, data)
			return ndata, nil
		}
	}
	return nil, fmt.Errorf("no such chunk/version combination: %d/%d", chunk, version)
}

func (m *MemoryStorage) WriteVersion(chunk apis.ChunkNum, version apis.Version, data []byte) error {
	m.assertOpen()
	if len(data) > apis.MaxChunkSize {
		return fmt.Errorf("chunk is too large: %d/%s = data[%d]", chunk, version, len(data))
	}
	versionMap := m.chunks[chunk]
	if versionMap == nil {
		versionMap = map[apis.Version][]byte{}
		m.chunks[chunk] = versionMap
	}
	existing, exists := versionMap[version]
	if exists {
		return fmt.Errorf("chunk/version combination already exists: %d/%d = data[%d]", chunk, version, len(existing))
	}
	ndata := make([]byte, len(data))
	copy(ndata, data)
	versionMap[version] = ndata
	return nil
}

func (m *MemoryStorage) DeleteVersion(chunk apis.ChunkNum, version apis.Version) error {
	m.assertOpen()
	versionMap := m.chunks[chunk]
	if versionMap == nil {
		return fmt.Errorf("chunk/version combination does not exist: %d/%d", chunk, version)
	}
	_, exists := versionMap[version]
	if !exists {
		return fmt.Errorf("chunk/version combination does not exist: %d/%d", chunk, version)
	}
	delete(versionMap, version)
	return nil
}

func (m *MemoryStorage) ListChunksWithLatest() ([]apis.ChunkNum, error) {
	m.assertOpen()
	result := make([]apis.ChunkNum, 0, len(m.latest))
	for k, _ := range m.latest {
		result = append(result, k)
	}
	return result, nil
}

func (m *MemoryStorage) GetLatestVersion(chunk apis.ChunkNum) (apis.Version, error) {
	m.assertOpen()
	if version, found := m.latest[chunk]; found {
		return version, nil
	}
	return 0, fmt.Errorf("no latest version for chunk: %d", chunk)
}

func (m *MemoryStorage) SetLatestVersion(chunk apis.ChunkNum, latest apis.Version) error {
	m.assertOpen()
	m.latest[chunk] = latest
	return nil
}

func (m *MemoryStorage) DeleteLatestVersion(chunk apis.ChunkNum) error {
	m.assertOpen()
	_, found := m.latest[chunk]
	if found {
		delete(m.latest, chunk)
		return nil
	} else {
		return fmt.Errorf("cannot delete nonexistent latest version for chunk: %d", chunk)
	}
}

func (m *MemoryStorage) Close() {
	m.chunks = nil
	m.latest = nil
	m.isClosed = true
}
