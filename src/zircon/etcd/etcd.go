package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"strconv"
	"strings"
	"sync"
	"time"
	"zircon/apis"
	"encoding/binary"
)

type etcdinterface struct {
	LocalName apis.ServerName
	Client    *clientv3.Client

	LeaseMutex sync.Mutex
	Lease      clientv3.LeaseID // TODO: ensure that Lease is still the same after each transaction
}

// Connects to etcd and provides our specific etcd interface based on that connection.
func SubscribeEtcd(localName apis.ServerName, servers []apis.ServerAddress) (apis.EtcdInterface, error) {
	endpoints := make([]string, len(servers))
	for i, v := range servers {
		endpoints[i] = string(v)
	}
	client, err := clientv3.NewFromURLs(endpoints)
	if err != nil {
		return nil, err
	}
	return &etcdinterface{
		LocalName: localName,
		Client:    client,
	}, nil
}

func (e *etcdinterface) GetName() apis.ServerName {
	return e.LocalName
}

func typeToString(kind apis.ServerType) string {
	switch kind {
	case apis.FRONTEND:
		return "frontend"
	case apis.CHUNKSERVER:
		return "chunkserver"
	case apis.METADATACACHE:
		return "metadatacache"
	default:
		panic("invalid server type")
	}
}

func (e *etcdinterface) GetAddress(name apis.ServerName, kind apis.ServerType) (apis.ServerAddress, error) {
	response, err := e.Client.Get(context.Background(), "/server/addresses/"+typeToString(kind)+"/"+string(name))
	if err != nil {
		return "", err
	}
	if len(response.Kvs) == 0 {
		return "", fmt.Errorf("no address for server %s with type %s", name, typeToString(kind))
	}
	return apis.ServerAddress(response.Kvs[0].Value), nil
}

func (e *etcdinterface) ListServers(kind apis.ServerType) ([]apis.ServerName, error) {
	start := "/server/addresses/" + typeToString(kind) + "/"
	end := "/server/addresses/" + typeToString(kind) + "0" // because '0' is the character directly after '/'
	response, err := e.Client.Get(context.Background(), start, clientv3.WithRange(end), clientv3.WithLimit(0), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	if response.More {
		return nil, errors.New("etcd refused to return all results at once")
	}
	var results []apis.ServerName
	for _, kv := range response.Kvs {
		if !strings.HasPrefix(string(kv.Key), start) {
			return nil, fmt.Errorf("unexpected key in result: '%s' when prefix was '%s'", string(kv.Key), start)
		}
		results = append(results, apis.ServerName(kv.Key[len(start):]))
	}
	return results, nil
}

// Note: if the server crashes after calling this and before using the result, a server ID could be skipped.
func (e *etcdinterface) getNextIndex() (apis.ServerID, error) {
	for {
		resp, err := e.Client.Get(context.Background(), "/server/next-id")
		if err != nil {
			return 0, err
		}
		if len(resp.Kvs) > 0 {
			lastId, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 32)
			if err != nil {
				return 0, err
			}
			nextId := lastId + 1
			resp, err := e.Client.Txn(context.Background()).
				If(clientv3.Compare(clientv3.Value("/server/next-id"), "=", string(resp.Kvs[0].Value))).
				Then(clientv3.OpPut("/server/next-id", strconv.FormatUint(uint64(nextId), 10))).
				Commit()
			if err != nil {
				return 0, err
			}
			if resp.Succeeded {
				return apis.ServerID(nextId), nil
			}
			// changed... try again
		} else {
			resp, err := e.Client.Txn(context.Background()).
				If(clientv3.Compare(clientv3.CreateRevision("/server/next-id"), "=", 0)).
				Then(clientv3.OpPut("/server/next-id", "1")).
				Commit()
			if err != nil {
				return 0, err
			}
			if resp.Succeeded {
				return 1, nil
			}
			// changed... try again
		}
	}
}

// Looks up the ID for the name, or zero if none is assigned; if an ID is assigned but the datastructures are
// inconsistent, will also fix the datastructures.
func (e *etcdinterface) getAndCorrectIdForName(name apis.ServerName) (apis.ServerID, error) {
	byName := fmt.Sprintf("/server/by-name/%s", name)

	resp, err := e.Client.Get(context.Background(), byName)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	} else {
		id, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 32)
		if err != nil {
			return 0, err
		}
		byId := fmt.Sprintf("/server/by-id/%d", id)
		resp, err = e.Client.Get(context.Background(), byId)
		if err != nil {
			return 0, err
		}
		if len(resp.Kvs) == 0 {
			// by-id mapping is missing; repopulate it
			_, err := e.Client.Put(context.Background(), byId, string(name))
			if err != nil {
				return 0, err
			}
		} else {
			if string(resp.Kvs[0].Value) != string(name) {
				panic("mismatched name")
			}
		}
		return apis.ServerID(id), nil
	}
}

func (e *etcdinterface) UpdateAddress(address apis.ServerAddress, kind apis.ServerType) error {
	_, err := e.Client.Put(context.Background(), "/server/addresses/"+typeToString(kind)+"/"+string(e.LocalName), string(address))

	id, err := e.getAndCorrectIdForName(e.LocalName)
	if err != nil {
		return err
	}
	if id == 0 {
		id, err = e.getNextIndex()
		if err != nil {
			return err
		}
		byName := fmt.Sprintf("/server/by-name/%s", e.LocalName)
		byId := fmt.Sprintf("/server/by-id/%d", id)

		_, err = e.Client.Put(context.Background(), byName, strconv.FormatUint(uint64(id), 10))
		if err != nil {
			return err
		}
		_, err = e.Client.Put(context.Background(), byId, string(e.LocalName))
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *etcdinterface) GetNameByID(id apis.ServerID) (apis.ServerName, error) {
	result, err := e.Client.Get(context.Background(), fmt.Sprintf("/server/by-id/%d", id))
	if err != nil {
		return "", err
	}
	if len(result.Kvs) == 0 {
		return "", fmt.Errorf("no such server ID: %d", id)
	}
	return apis.ServerName(result.Kvs[0].Value), nil
}

func (e *etcdinterface) GetIDByName(name apis.ServerName) (apis.ServerID, error) {
	result, err := e.Client.Get(context.Background(), fmt.Sprintf("/server/by-name/%s", name))
	if err != nil {
		return 0, err
	}
	if len(result.Kvs) == 0 {
		return 0, fmt.Errorf("no such server name: %s", name)
	}
	id, err := strconv.ParseUint(string(result.Kvs[0].Value), 10, 32)
	if err != nil {
		return 0, err
	}
	return apis.ServerID(id), nil
}

const TTL int64 = 1

func (e *etcdinterface) GetMetadataLeaseTimeout() time.Duration {
	return time.Second * time.Duration(TTL)
}

func (e *etcdinterface) RenewMetadataClaims() error {
	e.LeaseMutex.Lock()
	defer e.LeaseMutex.Unlock()
	if e.Lease == clientv3.NoLease {
		return errors.New("no lease exists (or already lost)")
	}
	resp, err := e.Client.KeepAliveOnce(context.Background(), e.Lease)
	if err != nil {
		// TODO: is this the right action to take?
		e.Lease = clientv3.NoLease
		return err
	}
	if resp.TTL < 1 {
		panic("expected positive TTL!")
	}
	return nil
}

func (e *etcdinterface) BeginMetadataLease() error {
	e.LeaseMutex.Lock()
	defer e.LeaseMutex.Unlock()
	if e.Lease != clientv3.NoLease {
		return errors.New("attempt to begin metadata lease when lease already exists!")
	}
	resp, err := e.Client.Grant(context.Background(), TTL)
	if err != nil {
		return err
	}
	e.Lease = resp.ID
	if e.Lease == clientv3.NoLease {
		panic("no lease???")
	}
	return nil
}

func (e *etcdinterface) TryClaimingMetadata(blockid apis.MetadataID) (apis.ServerName, error) {
	lease := func() clientv3.LeaseID {
		e.LeaseMutex.Lock()
		defer e.LeaseMutex.Unlock()
		return e.Lease
	}()
	if lease == clientv3.NoLease {
		return "", errors.New("no configured lease")
	}

	key := fmt.Sprintf("/metadata/claims/%d", blockid)

	txn, err := e.Client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(e.LocalName), clientv3.WithLease(lease))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return "", err
	}
	// We ensure that our lease is still active before returning anything.
	if err := e.RenewMetadataClaims(); err != nil {
		return "", err
	}
	if txn.Succeeded {
		// We've got it!
		return e.LocalName, nil
	} else {
		// We didn't get it. (Or maybe we already had it -- who knows?)
		// But in either case, we should just return who DOES have it.
		kv := txn.Responses[0].GetResponseRange().Kvs[0]
		if string(kv.Key) != key {
			panic("mismatched internal result")
		}
		return apis.ServerName(kv.Value), nil
	}
}

// Lists the MetadataIDs of every metadata block that exists
// TODO Clean up this and the next function by deduplicating
// the code that finds all metaids
func (e *etcdinterface) ListAllMetaIDs() ([]apis.MetadataID, error) {
	start := "/metadata/"
	end := "/metadata0" // because '0' is the character directly after '/'
	resp, err := e.Client.Get(context.Background(), start, clientv3.WithRange(end), clientv3.WithKeysOnly())
	if err != nil {
		return []apis.MetadataID{}, err
	}
	// scan through to find all blocks
	all := []apis.MetadataID{}
	for _, kv := range resp.Kvs {
		if strings.HasPrefix(string(kv.Key), "/metadata/data/") {
			metaID, err := strconv.ParseUint(string(kv.Key[len("/metadata/data/"):]), 10, 64)
			if err != nil {
				return []apis.MetadataID{}, err
			}
			all = append(all, apis.MetadataID(metaID))
		}
	}

	return all, nil
}

// Try claiming some block of metametadata that is available and report how many remaining blocks are available to be leased
func (e *etcdinterface) LeaseAnyMetametadata() (apis.MetadataID, error) {
	start := "/metadata/"
	end := "/metadata0" // because '0' is the character directly after '/'
	resp, err := e.Client.Get(context.Background(), start, clientv3.WithRange(end), clientv3.WithKeysOnly())
	if err != nil {
		return 0, err
	}
	// scan through to find all blocks
	all := map[apis.MetadataID]bool{}
	for _, kv := range resp.Kvs {
		if strings.HasPrefix(string(kv.Key), "/metadata/data/") {
			metaID, err := strconv.ParseUint(string(kv.Key[len("/metadata/data/"):]), 10, 64)
			if err != nil {
				return 0, err
			}
			all[apis.MetadataID(metaID)] = true
		}
	}
	// scan through to eliminate everything already claimed
	for _, kv := range resp.Kvs {
		if strings.HasPrefix(string(kv.Key), "/metadata/claims/") {
			metaID, err := strconv.ParseUint(string(kv.Key[len("/metadata/claims/"):]), 10, 64)
			if err != nil {
				return 0, err
			}
			all[apis.MetadataID(metaID)] = false
		}
	}

	for k, v := range all {
		if v {
			owner, err := e.TryClaimingMetadata(k)
			if err != nil {
				return 0, err
			}
			// did we grab it?
			if owner == e.GetName() {
				// yup!
				return k, nil
			}
			// continue and try the next option
		}
	}

	// nothing left to claim
	return 0, nil
}

// Assuming that this server owns a particular block of metadata, release that metadata back out into the wild.
func (e *etcdinterface) DisclaimMetadata(blockid apis.MetadataID) error {
	key := fmt.Sprintf("/metadata/claims/%d", blockid)

	txn, err := e.Client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.Value(key), "=", string(e.LocalName))).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		return errors.New("metadata was not claimed in the first place!")
	}
	return nil
}

func (e *etcdinterface) getMetametadataRaw(blockid apis.MetadataID) ([]byte, apis.MetadataEntry, error) {
	checkKey := fmt.Sprintf("/metadata/claims/%d", blockid)
	readKey := fmt.Sprintf("/metadata/data/%d", blockid)

	txn, err := e.Client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.Value(checkKey), "=", string(e.LocalName))).
		Then(clientv3.OpGet(readKey)).
		Commit()
	if err != nil {
		return nil, apis.MetadataEntry{}, err
	}
	if !txn.Succeeded {
		return nil, apis.MetadataEntry{}, errors.New("cannot get metadata; claim not held by us")
	}

	kvs := txn.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		// just return an empty block by default
		return nil, apis.MetadataEntry{
			Replicas: nil,
		}, nil
	} else {
		// otherwise return ACTUAL DATA
		mmd := apis.MetadataEntry{}
		err := json.Unmarshal(kvs[0].Value, &mmd)
		if err != nil {
			return nil, apis.MetadataEntry{}, err
		}
		return kvs[0].Value, mmd, nil
	}
}

func (e *etcdinterface) GetMetametadata(blockid apis.MetadataID) (apis.MetadataEntry, error) {
	_, entry, err := e.getMetametadataRaw(blockid)
	return entry, err
}

func (e *etcdinterface) UpdateMetametadata(blockid apis.MetadataID, previous apis.MetadataEntry, data apis.MetadataEntry) error {
	checkKey := fmt.Sprintf("/metadata/claims/%d", blockid)
	readKey := fmt.Sprintf("/metadata/data/%d", blockid)

	originalBytes, originalEntry, err := e.getMetametadataRaw(blockid)
	if err != nil {
		return err
	}

	if !originalEntry.Equals(previous) {
		return errors.New("cannot update metadata; mismatch on previous value")
	}

	menc, err := json.Marshal(data)
	if err != nil {
		return err
	}

	var checkPrevious clientv3.Cmp
	if originalBytes == nil {
		checkPrevious = clientv3.Compare(clientv3.CreateRevision(readKey), "=", 0)
	} else {
		checkPrevious = clientv3.Compare(clientv3.Value(readKey), "=", string(originalBytes))
	}

	txn, err := e.Client.Txn(context.Background()).
		If(clientv3.Compare(clientv3.Value(checkKey), "=", string(e.LocalName)),
			checkPrevious).
		Then(clientv3.OpPut(readKey, string(menc))).
		Else(clientv3.OpGet(checkKey)).
		Commit()
	if err != nil {
		return err
	}
	if !txn.Succeeded {
		kvs := txn.Responses[0].GetResponseRange().Kvs
		if len(kvs) == 0 {
			return errors.New("cannot update metadata; claim not held")
		}
		if string(kvs[0].Value) != string(e.LocalName) {
			return errors.New("cannot update metadata; claim held by someone else")
		}
		return errors.New("cannot update metadata; data-level mismatch")
	}
	return nil
}

func (e *etcdinterface) Close() error {
	return e.Client.Close()
}

// SyncServer-related code

const FilesystemNextSyncKey = "/fs/nextsync"

func encodeSync(id apis.SyncID) string {
	bin := make([]byte, 8)
	binary.LittleEndian.PutUint64(bin, uint64(id))
	return string(bin)
}

func encodeChunk(chunk apis.ChunkNum) string {
	bin := make([]byte, 8)
	binary.LittleEndian.PutUint64(bin, uint64(chunk))
	return string(bin)
}

func decodeChunk(c []byte) apis.ChunkNum {
	return apis.ChunkNum(binary.LittleEndian.Uint64(c))
}

const NoSync apis.SyncID = 0

func (e *etcdinterface) nextSyncID() (apis.SyncID, error) {
	resp, err := e.Client.Get(context.Background(), FilesystemNextSyncKey)
	if err != nil {
		return 0, err
	}
	kvs := resp.Kvs
	for {
		var lastID apis.SyncID
		txn := e.Client.Txn(context.Background())
		if len(kvs) != 0 {
			lastID = apis.SyncID(binary.LittleEndian.Uint64(kvs[0].Value))
			txn = txn.If(clientv3.Compare(clientv3.Value(FilesystemNextSyncKey), "=", string(kvs[0].Value)))
		} else {
			txn = txn.If(clientv3.Compare(clientv3.CreateRevision(FilesystemNextSyncKey), "=", 0))
		}
		lastID += 1
		tresp, err := txn.
			Then(clientv3.OpPut(FilesystemNextSyncKey, encodeSync(lastID))).
			Else(clientv3.OpGet(FilesystemNextSyncKey)).
			Commit()
		if err != nil {
			return 0, err
		}
		if tresp.Succeeded {
			return lastID, nil
		}
		kvs = tresp.Responses[0].GetResponseRange().Kvs
		// try again!
	}
}
