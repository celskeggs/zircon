package filesystem

import (
	"zircon/apis"
	"encoding/binary"
	"zircon/util"
	"github.com/pkg/errors"
	"fmt"
	path2 "path"
)

type Traverser struct {
	client apis.Client
	fs FilesystemSync
}

// Each of the following structures inherently includes a READ LOCK. You can assume the item itself will not change!
// Relatedly, do not hold onto references to these for long periods of time.

type Reference struct {
	t        Traverser
	chunk    apis.ChunkNum
	unlocker Unlocker
}

type File struct {
	t        Traverser
	chunk    apis.ChunkNum
	unlocker Unlocker
}

type NodeType uint8
const (
	NONEXISTENT NodeType = iota
	FILE NodeType = iota
	DIRECTORY NodeType = iota
	SYMLINK NodeType = iota
)

func (t Traverser) Root() (*Reference, error) {
	root, err := t.fs.GetRoot()
	if err != nil {
		return nil, err
	}
	lock, err := t.fs.ReadLockChunk(root)
	if err != nil {
		return nil, err
	}
	return &Reference{
		chunk: root,
		unlocker: lock,
		t: t,
	}, nil
}

func splitPathMany(path string) []string {
	if path[0] != '/' {
		panic("invalid absolute path to splitPathMany")
	}
	var components []string
	for path != "/" {
		if path[len(path) - 1] == '/' {
			path = path[:len(path) - 1]
		}
		dir, base := path2.Dir(path), path2.Base(path)
		path = dir
		components = append(components, base)
	}
	reverse := make([]string, len(components))
	for i, elem := range components {
		reverse[len(reverse) - i - 1] = elem
	}
	return reverse
}

func (t Traverser) PathDir(path string) (*Reference, error) {
	if path[0] != '/' {
		return nil, errors.New("path is not absolute!")
	}
	// TODO: traverse symlinks
	directory, err := t.Root()
	if err != nil {
		return nil, err
	}
	for _, elem := range splitPathMany(path) {
		// invariant: each time around the loop, we have exactly one lock, which is a read lock on 'directory'
		ndir, err := directory.LookupDir(elem)
		directory.Release()
		if err != nil {
			return nil, err
		}
		directory = ndir
	}
	return directory, nil
}

const EntrySize = 32
const MaxName = EntrySize - 8 - 1
const EntryCount = apis.MaxChunkSize / EntrySize
const MaxSymLinkSize = 1024

type Entry struct {
	Index int        // not stored in encoding; broadly optional
	Type  NodeType
	Name  string
	Chunk apis.ChunkNum
}

func (e *Entry) IsOk() bool {
	return ((e.Type == FILE || e.Type == DIRECTORY || e.Type == SYMLINK) && (e.Chunk != 0) && (len(e.Name) > 0)) || e.Type == NONEXISTENT
}

func decode(data []byte, index int) Entry {
	return Entry {
		Index: index,
		Type: NodeType(data[0]),
		Chunk: apis.ChunkNum(binary.LittleEndian.Uint64(data[1:])),
		Name: string(util.StripTrailingZeroes(data[9:])),
	}
}

func (e *Entry) encode() ([]byte, error) {
	result := make([]byte, EntrySize)
	result[0] = uint8(e.Type)
	binary.LittleEndian.PutUint64(result[1:9], uint64(e.Chunk))
	if len(e.Name) > MaxName {
		return nil, errors.New("filename in entry is too long!")
	}
	copy(result[9:], e.Name)
	return result, nil
}

// results are in sorted order by index
func (r *Reference) listEntries() ([]Entry, apis.Version, error) {
	if err := r.unlocker.Ensure(); err != nil {
		return nil, 0, err
	}
	data, ver, err := r.t.client.Read(r.chunk, 0, apis.MaxChunkSize)
	if err != nil {
		return nil, 0, err
	}
	var result []Entry
	for i := 0; i < EntryCount; i++ {
		entry := decode(data[i *EntrySize:i *EntrySize+EntrySize], i)
		if !entry.IsOk() {
			return nil, 0, errors.New("found invalid entry in folder!")
		}
		if entry.Type != NONEXISTENT {
			result = append(result, entry)
		}
	}
	return result, ver, nil
}

func (r *Reference) elevated() (*Reference, error) {
	nul, err := r.unlocker.Elevate()
	if err != nil {
		return nil, err
	}
	return &Reference{
		unlocker: nul,
		t: r.t,
		chunk: r.chunk,
	}, nil
}

func (r *Reference) updateEntry(version apis.Version, index int, new Entry) (apis.Version, error) {
	if err := r.unlocker.EnsureWrite(); err != nil {
		return 0, err
	}
	data, err := new.encode()
	if err != nil {
		return 0, err
	}
	return r.t.client.Write(r.chunk, uint32(index * EntrySize), version, data)
}

func (r *Reference) Stat(name string) (NodeType, error) {
	if name == "" {
		return NONEXISTENT, errors.New("empty filename")
	}
	entries, _, err := r.listEntries()
	if err != nil {
		return NONEXISTENT, err
	}
	for _, entry := range entries {
		if entry.Name == name {
			return entry.Type, nil
		}
	}
	return NONEXISTENT, nil
}

func (r *Reference) lookupEntryAny(name string) (Entry, int, apis.Version, error) {
	if name == "" {
		return Entry{}, 0, 0, errors.New("empty filename")
	}
	entries, ver, err := r.listEntries()
	if err != nil {
		return Entry{}, 0, ver, err
	}
	for index, entry := range entries {
		if entry.Name == name {
			return entry, index, 0, nil
		}
	}
	return Entry{}, 0, ver, fmt.Errorf("no such node: %s", name)
}

func (r *Reference) lookupEntry(name string, ntype NodeType) (Entry, error) {
	entry, _, _, err := r.lookupEntryAny(name)
	if err != nil {
		return Entry{}, err
	}
	if entry.Type != ntype {
		return Entry{}, fmt.Errorf("bad file type for: %s", name)
	}
	return entry, nil
}

func (r *Reference) LookupFile(name string) (*File, error) {
	entry, err := r.lookupEntry(name, FILE)
	if err != nil {
		return nil, err
	}
	unlocker, err := r.t.fs.ReadLockChunk(entry.Chunk)
	if err != nil {
		return nil, err
	}
	return &File{
		chunk: entry.Chunk,
		unlocker: unlocker,
		t: r.t,
	}, nil
}

func (r *Reference) LookupDir(name string) (*Reference, error) {
	entry, err := r.lookupEntry(name, DIRECTORY)
	if err != nil {
		return nil, err
	}
	unlocker, err := r.t.fs.ReadLockChunk(entry.Chunk)
	if err != nil {
		return nil, err
	}
	return &Reference{
		chunk: entry.Chunk,
		unlocker: unlocker,
		t: r.t,
	}, nil
}

func (r *Reference) LookupSymLink(name string) (string, error) {
	entry, err := r.lookupEntry(name, FILE)
	if err != nil {
		return "", err
	}
	unlocker, err := r.t.fs.ReadLockChunk(entry.Chunk)
	if err != nil {
		return "", err
	}
	file := &File{
		chunk: entry.Chunk,
		unlocker: unlocker,
		t: r.t,
	}
	defer file.Release()
	data, err := file.Read(0, MaxSymLinkSize)
	if err != nil {
		return "", err
	}
	return string(util.StripTrailingZeroes(data)), nil
}

func (r *Reference) scanNewEntry(name string) (int, apis.Version, error) {
	if name == "" {
		return 0, 0, errors.New("empty filename")
	}
	if len(name) > MaxName {
		return 0, 0, fmt.Errorf("name too long")
	}
	entries, ver, err := r.listEntries()
	if err != nil {
		return 0, 0, err
	}
	firstFree := 0
	for _, entry := range entries {
		if entry.Name == name {
			return 0, 0, fmt.Errorf("file already exists: %s", name)
		}
		if entry.Index == firstFree {
			firstFree++ // lets firstFree land on the first empty entry
		}
	}
	if firstFree >= EntryCount {
		return 0, 0, errors.New("no room in directory for another file")
	}
	return firstFree, ver, nil
}

func (r *Reference) tryNewEntry(name string, exec func () (apis.ChunkNum, NodeType, error)) (error) {
	firstFree, ver, err := r.scanNewEntry(name)
	if err != nil {
		return err
	}
	elevated, err := r.elevated()
	if err != nil {
		return err
	}
	defer elevated.Release()
	chunk, ntype, err := exec()
	if err != nil {
		return err
	}
	// TODO: what if we crash here
	_, err = elevated.updateEntry(ver, firstFree, Entry{
		Chunk: chunk,
		Type: ntype,
		Name: name,
	})
	return err
}

func (r *Reference) NewFile(name string) error {
	return r.tryNewEntry(name, func() (apis.ChunkNum, NodeType, error) {
		chunk, err := r.t.client.New()
		return chunk, FILE, err
	})
}

func (r *Reference) NewDir(name string) error {
	return r.tryNewEntry(name, func() (apis.ChunkNum, NodeType, error) {
		chunk, err := r.t.client.New()
		return chunk, DIRECTORY, err
	})
}

func (r *Reference) NewSymLink(name string, target string) error {
	if len(target) > MaxSymLinkSize {
		return errors.New("symlink too long")
	}
	return r.tryNewEntry(name, func() (apis.ChunkNum, NodeType, error) {
		chunk, err := r.t.client.New()
		if err != nil {
			return 0, NONEXISTENT, err
		}
		_, err = r.t.client.Write(chunk, 0, apis.AnyVersion, []byte(target))
		if err != nil {
			return 0, NONEXISTENT, err
		}
		return chunk, SYMLINK, nil
	})
}

// attempts to elevate two different references at once.
// this gets around the normal rule of "only one elevated reference at a time" by ordering based on chunk number
func elevateBoth(r1, r2 *Reference) (*Reference, *Reference, error) {
	flip := r1.chunk > r2.chunk
	if flip {
		r1, r2 = r2, r1
	}
	r1e, err := r1.elevated()
	if err != nil {
		return nil, nil, err
	}
	r2e, err := r2.elevated()
	if err != nil {
		r1e.Release()
		return nil, nil, err
	}
	if flip {
		return r2e, r1e, nil
	} else {
		return r1e, r2e, nil
	}
}

func (r *Reference) Rename(sourcename string, targetname string) error {
	if sourcename == targetname {
		return errors.New("attempt to rename file to itself!")
	}
	entryS, indexS, verS, err := r.lookupEntryAny(sourcename)
	if err != nil {
		return err
	}
	indexT, _, err := r.scanNewEntry(targetname)
	if err != nil {
		return err
	}
	elevated, err := r.elevated()
	if err != nil {
		return err
	}
	defer elevated.Release()
	verN, err := elevated.updateEntry(verS, indexS, Entry{ Type: NONEXISTENT })
	if err != nil {
		return err
	}
	// TODO: this point contains a serious concurrency flaw: a race condition that can make a file disappear!
	//       THIS NEEDS TO BE FIXED.
	if _, err = elevated.updateEntry(verN, indexT, entryS); err != nil {
		return err
	}
	return nil
}

func (r *Reference) MoveTo(target *Reference, sourcename string, targetname string) error {
	if r.chunk == target.chunk {
		return r.Rename(sourcename, targetname)
	}
	entryS, indexS, verS, err := r.lookupEntryAny(sourcename)
	if err != nil {
		return err
	}
	indexT, verT, err := target.scanNewEntry(targetname)
	if err != nil {
		return err
	}
	elevSource, elevTarget, err := elevateBoth(r, target)
	if err != nil {
		return err
	}
	defer elevSource.Release()
	defer elevTarget.Release()
	if _, err = elevSource.updateEntry(verS, indexS, Entry{ Type: NONEXISTENT }); err != nil {
		return err
	}
	// TODO: this point contains a serious concurrency flaw: a race condition that can make a file disappear!
	//       THIS NEEDS TO BE FIXED.
	if _, err = elevTarget.updateEntry(verT, indexT, entryS); err != nil {
		return err
	}
	return nil
}

func (r *Reference) Remove(name string, rmdir bool) error {
	entry, index, ver, err := r.lookupEntryAny(name)
	if err != nil {
		return err
	}
	if entry.Type == DIRECTORY {
		if !rmdir {
			return errors.New("attempt to remove directory")
		}
		dir, err := r.LookupDir(name)
		if err != nil {
			return err
		}
		defer dir.Release()
		contents, _, err := dir.listEntries()
		if err != nil {
			return err
		}
		if len(contents) != 0 {
			return errors.New("attempt to remove non-empty directory")
		}
		// TODO: check this ordering of elevation -- is there a deadlock here?
		dirElevated, err := dir.elevated()
		if err != nil {
			return err
		}
		defer dirElevated.Release()
		// (we just need to hold this until we perform the delete at the end)
	} else {
		if rmdir {
			return errors.New("attempt to remove non-directory")
		}
		unlocker, err := r.t.fs.WriteLockChunk(r.chunk)
		if err != nil {
			return err
		}
		defer unlocker.Unlock()
	}
	elevated, err := r.elevated()
	if err != nil {
		return err
	}
	defer elevated.Release()
	if _, err = elevated.updateEntry(ver, index, Entry{Type: NONEXISTENT}); err != nil {
		return err
	}
	// TODO: check failure modes here
	return elevated.t.client.Delete(entry.Chunk, apis.AnyVersion)
}

func (r *Reference) Release() {
	r.unlocker.Unlock()
}

// TODO: use caching... we're allowed to, since we have a read lock!
func (f *File) Size() (uint32, error) {
	if err := f.unlocker.Ensure(); err != nil {
		return 0, err
	}
	// file chunks include an embedded length field at the start
	binlength, _, err := f.t.client.Read(f.chunk, 0, 4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(binlength), nil
}

func (f *File) Read(offset uint32, length uint32) ([]byte, error) {
	if err := f.unlocker.Ensure(); err != nil {
		return nil, err
	}
	if offset + 4 < offset {
		return nil, errors.New("offset too large; overflow")
	}
	data, _, err := f.t.client.Read(f.chunk, 0, offset + length + 4)
	if err != nil {
		return nil, err
	}
	maxlen := binary.LittleEndian.Uint32(data[0:4])
	if maxlen + 4 >= offset + 4 {
		return data[offset + 4:maxlen + 4], nil
	} else {
		return nil, nil
	}
}

func (f *File) Write(offset uint32, data []byte) error {
	// note: we do *not* elevate here! this is because POSIX supports parallel writes to the same file!
	if err := f.unlocker.Ensure(); err != nil {
		return err
	}
	binlength, ver, err := f.t.client.Read(f.chunk, 0, 4)
	if err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(binlength)
	dlen := uint32(len(data))
	// TODO: come back and check integer overflow cases
	if offset + dlen > length {
		// this means we need to update the length, not just the data
		if offset > length {
			// this means we need to write a block of zeroes too
			padded := make([]byte, offset + dlen - length)
			copy(padded[offset - length:], data)
			ver, err = f.t.client.Write(f.chunk, 4 + length, ver, padded)
			if err != nil {
				return err
			}
		} else {
			ver, err = f.t.client.Write(f.chunk, 4 + offset, ver, data)
			if err != nil {
				return err
			}
		}
		// now fix the length (note: this should retry on its own)
		nbinlength := make([]byte, 4)
		binary.LittleEndian.PutUint32(nbinlength, offset + dlen)
		_, err = f.t.client.Write(f.chunk, 0, ver, nbinlength)
		if err != nil {
			return err
		}
	} else {
		_, err = f.t.client.Write(f.chunk, 4 + offset, ver, data)
		if err != nil {
			// TODO: retry on version mismatch failure (for all)
			return err
		}
	}
	return nil
}

func (f *File) Truncate(nlength uint32) error {
	// note: we do *not* elevate here! this is because POSIX supports parallel writes to the same file!
	if err := f.unlocker.Ensure(); err != nil {
		return err
	}
	binlength, ver, err := f.t.client.Read(f.chunk, 0, 4)
	if err != nil {
		return err
	}
	length := binary.LittleEndian.Uint32(binlength)
	if nlength == length {
		return nil
	}
	if nlength > length { // needs to be zeroed out first
		ver, err = f.t.client.Write(f.chunk, length, ver, make([]byte, nlength - length))
		if err != nil {
			// TODO: maybe retry
			return err
		}
	}
	// now we can just adjust the length
	nbinlength := make([]byte, 4)
	binary.LittleEndian.PutUint32(nbinlength, nlength)
	_, err = f.t.client.Write(f.chunk, 0, ver, nbinlength)
	if err != nil {
		// TODO: maybe retry
		return err
	}
	return nil
}

func (f *File) Release() {
	f.unlocker.Unlock()
}
