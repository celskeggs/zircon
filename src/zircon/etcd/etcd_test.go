package etcd

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"zircon/apis"
)

func newConfig(dirname string, serverURL string, advertiseURL string) (*embed.Config, error) {
	ec := embed.NewConfig()

	listenURL, err := url.Parse(serverURL)
	if err != nil {
		return nil, err
	}
	clientURL, err := url.Parse(advertiseURL)
	if err != nil {
		return nil, err
	}
	ec.TickMs = 1
	ec.ElectionMs = 10
	ec.LPUrls = []url.URL{*listenURL}
	ec.APUrls = []url.URL{*listenURL}
	ec.LCUrls = []url.URL{*clientURL}
	ec.ACUrls = []url.URL{*clientURL}

	ec.Dir = dirname

	ec.InitialCluster = ec.InitialClusterFromName(ec.Name)

	return ec, ec.Validate()
}

func LaunchTestingEtcdServer() (string, func() error, error) {
	dirname, err := ioutil.TempDir("", "etcd.datadir.")
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(dirname)

	rport := 32768 + rand.Intn(32767)

	clientURL := fmt.Sprintf("http://127.0.0.3:%d", rport)

	cfg, err := newConfig(dirname, fmt.Sprintf("http://127.0.0.2:%d", rport), clientURL)
	if err != nil {
		return "", nil, err
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return "", nil, err
	}
	select {
	case <-e.Server.ReadyNotify(): // wait for e.Server to join the cluster
	case <-e.Server.StopNotify(): // publish aborted from 'ErrStopped'
	}
	stopped, errc, err := e.Server.StopNotify(), e.Err(), nil

	if err != nil {
		return "", nil, err
	}

	abort := func() error {
		e.Close()
		select {
		case lerr := <-errc:
			return lerr
		case <-stopped:
			return nil
		}
	}

	return clientURL, abort, nil
}

func PrepareSubscribe(t *testing.T) (subscribe func(local apis.ServerName) (apis.EtcdInterface, func()), teardown func()) {
	server, abort, err := LaunchTestingEtcdServer()
	require.NoError(t, err)

	servers := []apis.ServerAddress{apis.ServerAddress(server)}

	return func(local apis.ServerName) (apis.EtcdInterface, func()) {
			iface, err := SubscribeEtcd(local, servers)
			require.NoError(t, err)
			return iface, func() {
				err := iface.Close()
				require.NoError(t, err)
			}
		}, func() {
			err := abort()
			require.NoError(t, err)
		}
}

// Just to make sure that our mechanism of launching etcd actually works.
func TestEtcdTesting(t *testing.T) {
	server, abort, err := LaunchTestingEtcdServer()
	if err != nil {
		t.Fatal(err)
	}
	defer abort()

	client, err := clientv3.NewFromURL(server)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.Put(context.Background(), "hello-world", "hello-human")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(context.Background(), "hello-world")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "hello-human", string(resp.Kvs[0].Value))
}

func TestGetName(t *testing.T) {
	sub, teardown0 := PrepareSubscribe(t)
	defer teardown0()

	iface1, teardown1 := sub("test-name")
	defer teardown1()
	iface2, teardown2 := sub("test-name-2")
	defer teardown2()

	assert.Equal(t, "test-name", string(iface1.GetName()))
	assert.Equal(t, "test-name-2", string(iface2.GetName()))
}

// TODO: additional tests and implementation required
