package storage

import (
	"errors"
	"zircon/apis"
	"sort"
	"fmt"
	"os"
	"io/ioutil"
	"strings"
	"strconv"
	"io"
)

// TODO: caching?

type FilesystemStorage struct {
	isClosed bool
	path     string
}

// Given a base path for storage of files in a modern filesystem, construct an interface by which a chunkserver can store
// chunks.
func ConfigureFilesystemStorage(basepath string) (ChunkStorage, error) {
	if fi, err := os.Stat(basepath); err != nil {
		return nil, err
	} else if !fi.IsDir() {
		return nil, errors.New("not a directory")
	}
	return &FilesystemStorage{
		path: basepath,
	}, nil
}

func (m *FilesystemStorage) assertOpen() {
	if m.isClosed {
		panic("attempt to use closed FilesystemStorage")
	}
}

func (m *FilesystemStorage) chunkDir(chunk apis.ChunkNum) string {
	return fmt.Sprintf("%s/chunk-%d", m.path, chunk)
}

func (m *FilesystemStorage) chunkFilename(chunk apis.ChunkNum, version apis.Version) string {
	return fmt.Sprintf("%s/chunk-%d/%d", m.path, chunk, version)
}

func (m *FilesystemStorage) latestFilename(chunk apis.ChunkNum) string {
	return fmt.Sprintf("%s/latest-%d", m.path, chunk)
}

func (m *FilesystemStorage) ListChunksWithData() ([]apis.ChunkNum, error) {
	m.assertOpen()
	fis, err := ioutil.ReadDir(m.path)
	if err != nil {
		return nil, err
	}
	var result []apis.ChunkNum
	for _, fi := range fis {
		if strings.HasPrefix(fi.Name(), "chunk-") {
			chunk, err := strconv.ParseUint(fi.Name()[6:], 10, 64)
			if err != nil {
				return nil, err
			}
			result = append(result, apis.ChunkNum(chunk))
		}
	}
	return result, nil
}

func (m *FilesystemStorage) ListVersions(chunk apis.ChunkNum) ([]apis.Version, error) {
	m.assertOpen()
	fis, err := ioutil.ReadDir(m.chunkDir(chunk))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []apis.Version
	for _, fi := range fis {
		chunk, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(result, apis.Version(chunk))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result, nil
}

func (m *FilesystemStorage) ReadVersion(chunk apis.ChunkNum, version apis.Version) ([]byte, error) {
	m.assertOpen()
	return ioutil.ReadFile(m.chunkFilename(chunk, version))
}

// based on ioutil.WriteFile
func writeFileNew(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func (m *FilesystemStorage) WriteVersion(chunk apis.ChunkNum, version apis.Version, data []byte) error {
	m.assertOpen()
	if len(data) > apis.MaxChunkSize {
		return fmt.Errorf("chunk is too large: %d/%s = data[%d]", chunk, version, len(data))
	}
	err := os.Mkdir(m.chunkDir(chunk), os.FileMode(0755))
	if err != nil && !os.IsExist(err) {
		return err
	}
	return writeFileNew(m.chunkFilename(chunk, version), data, os.FileMode(0644))
}

func (m *FilesystemStorage) DeleteVersion(chunk apis.ChunkNum, version apis.Version) error {
	m.assertOpen()
	err := os.Remove(m.chunkFilename(chunk, version))
	if err == nil {
		// we don't care if this succeeds
		_ = os.Remove(m.chunkDir(chunk))
	}
	return err
}

func (m *FilesystemStorage) ListChunksWithLatest() ([]apis.ChunkNum, error) {
	m.assertOpen()
	fis, err := ioutil.ReadDir(m.path)
	if err != nil {
		return nil, err
	}
	var result []apis.ChunkNum
	for _, fi := range fis {
		if strings.HasPrefix(fi.Name(), "latest-") {
			chunk, err := strconv.ParseUint(fi.Name()[7:], 10, 64)
			if err != nil {
				return nil, err
			}
			result = append(result, apis.ChunkNum(chunk))
		}
	}
	return result, nil
}

func (m *FilesystemStorage) GetLatestVersion(chunk apis.ChunkNum) (apis.Version, error) {
	m.assertOpen()
	data, err := ioutil.ReadFile(m.latestFilename(chunk))
	if err != nil {
		return 0, err
	}
	ver, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}
	return apis.Version(ver), nil
}

func (m *FilesystemStorage) SetLatestVersion(chunk apis.ChunkNum, latest apis.Version) error {
	m.assertOpen()
	return ioutil.WriteFile(m.latestFilename(chunk), []byte(fmt.Sprintln(latest)), os.FileMode(0644))
}

func (m *FilesystemStorage) DeleteLatestVersion(chunk apis.ChunkNum) error {
	m.assertOpen()
	return os.Remove(m.latestFilename(chunk))
}

func (m *FilesystemStorage) Close() {
	m.isClosed = true
}
