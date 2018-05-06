package etcd

import (
	"fmt"
	"github.com/coreos/etcd/embed"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"
	"zircon/apis"
)

const TestingLeaseTimeout = time.Second

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

func PrepareSubscribeForTesting(t *testing.T) (subscribe func(local apis.ServerName) (apis.EtcdInterface, func()), teardown func()) {
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
