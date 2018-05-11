package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"zircon/apis"
	"zircon/chunkserver"
	"zircon/chunkserver/control"
	"zircon/chunkserver/storage"
	"zircon/client"
	"zircon/client/demo"
	"zircon/rpc"
	"zircon/filesystem"
	"github.com/coreos/etcd/clientv3"
	"zircon/etcd"
	"zircon/frontend"
	"zircon/metadatacache"
	"zircon/filesystem/syncserver"
	"zircon/filesystem/fuse"
)

type Config struct {
	ServerName apis.ServerName
	Address     apis.ServerAddress

	StorageType string
	StoragePath string

	EtcdServers         []apis.ServerAddress
	ClientConfig        client.Configuration
	MountPoint          string
	SyncServerAddresses []apis.ServerAddress
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &Config{}
	err = yaml.NewDecoder(file).Decode(config)

	return config, err
}

func ConfigureChunkserverStorage(config *Config) (store storage.ChunkStorage, err error) {
	switch config.StorageType {
	case "":
		err = fmt.Errorf("no specified kind of storage for chunkserver")
	case "memory":
		store, err = storage.ConfigureMemoryStorage()
	case "filesystem":
		store, err = storage.ConfigureFilesystemStorage(config.StoragePath)
	case "block":
		store, err = storage.ConfigureBlockStorage(config.StoragePath)
	default:
		err = fmt.Errorf("no such storage type: %s\n", config.StorageType)
	}
	return store, err
}

func LaunchChunkserver(config *Config) error {
	conncache := rpc.NewConnectionCache()
	defer conncache.CloseAll()

	store, err := ConfigureChunkserverStorage(config)
	if err != nil {
		return err
	}
	defer store.Close()

	singleserver, teardown, err := control.ExposeChunkserver(store)
	if err != nil {
		return err
	}
	defer teardown()

	server, err := chunkserver.WithChatter(singleserver, conncache)
	if err != nil {
		return err
	}

	cli, err := etcd.SubscribeEtcd(config.ServerName, config.EtcdServers)
	if err != nil {
		return err
	}

	finish, address, err := rpc.PublishChunkserver(server, config.Address)
	if err != nil {
		return err
	}

	err = cli.UpdateAddress(address, apis.CHUNKSERVER)
	if err != nil {
		return err
	}

	return finish(false) // wait for server to finish
}

func LaunchFrontend(config *Config) error {
	conncache := rpc.NewConnectionCache()
	defer conncache.CloseAll()

	cli, err := etcd.SubscribeEtcd(config.ServerName, config.EtcdServers)
	if err != nil {
		return err
	}

	fe, err := frontend.ConstructFrontend(cli, conncache)
	if err != nil {
		return err
	}

	finish, address, err := rpc.PublishFrontend(fe, config.Address)
	if err != nil {
		return err
	}

	err = cli.UpdateAddress(address, apis.FRONTEND)
	if err != nil {
		return err
	}

	return finish(false) // wait for server to finish
}

func LaunchMetadataCache(config *Config) error {
	conncache := rpc.NewConnectionCache()
	defer conncache.CloseAll()

	cli, err := etcd.SubscribeEtcd(config.ServerName, config.EtcdServers)
	if err != nil {
		return err
	}

	mc, err := metadatacache.NewCache(conncache, cli)
	if err != nil {
		return err
	}

	finish, address, err := rpc.PublishMetadataCache(mc, config.Address)
	if err != nil {
		return err
	}

	err = cli.UpdateAddress(address, apis.METADATACACHE)
	if err != nil {
		return err
	}

	return finish(false) // wait for server to finish
}

func LaunchSyncServer(config *Config) error {
	conncache := rpc.NewConnectionCache()
	defer conncache.CloseAll()

	cli, err := etcd.SubscribeEtcd(config.ServerName, config.EtcdServers)
	if err != nil {
		return err
	}

	blkclient, err := client.ConfigureNetworkedClient(config.ClientConfig)
	if err != nil {
		return err
	}

	ss := syncserver.NewSyncServer(cli, blkclient)

	finish, _, err := rpc.PublishSyncServer(ss, config.Address)
	if err != nil {
		return err
	}

	return finish(false) // wait for server to finish
}

func LaunchFuse(config *Config) error {
	return fuse.MountFuse(filesystem.Configuration{
		ClientConfig: config.ClientConfig,
		MountPoint: config.MountPoint,
		SyncServerAddresses: config.SyncServerAddresses,
	})
}

func LaunchDemoClient(config *Config) error {
	conncache := rpc.NewConnectionCache()
	defer conncache.CloseAll()

	clientConnection, err := client.ConfigureClient(config.ClientConfig, conncache)
	if err != nil {
		return err
	}

	return demo.LaunchDemo(clientConnection)
}

// parses out command-line arguments, determines what kind of server to run, then calls all of the relevant construction
// functions to build the relevant kind of server.
func main() {
	if len(os.Args) != 3 {
		fmt.Printf("usage: %s <config-path> <subprogram>\n", os.Args[0])
		fmt.Printf("Subprograms:\n")
		fmt.Printf(" - chunkserver\n")
		fmt.Printf(" - demo-client\n")
		fmt.Printf(" - frontend\n")
		fmt.Printf(" - fuse\n")
		fmt.Printf(" - metadata-cache\n")
		fmt.Printf(" - sync-server\n")
		os.Exit(1)
	}

	config, err := LoadConfig(os.Args[1])
	if err != nil {
		fmt.Printf("failed to load config: %v\n", err)
		os.Exit(1)
	}

	switch os.Args[2] {
	case "chunkserver":
		err := LaunchChunkserver(config)
		if err != nil {
			fmt.Printf("chunkserver terminated: %v\n", err)
			os.Exit(1)
		}
	case "frontend":
		err := LaunchFrontend(config)
		if err != nil {
			fmt.Printf("frontend terminated: %v\n", err)
			os.Exit(1)
		}
	case "metadata-cache":
		err := LaunchMetadataCache(config)
		if err != nil {
			fmt.Printf("metadata cache terminated: %v\n", err)
			os.Exit(1)
		}
	case "sync-server":
		err := LaunchSyncServer(config)
		if err != nil {
			fmt.Printf("sync server terminated: %v\n", err)
			os.Exit(1)
		}
	case "fuse":
		err := LaunchFuse(config)
		if err != nil {
			fmt.Printf("fuse terminated: %v\n", err)
			os.Exit(1)
		}
	case "demo-client":
		err := LaunchDemoClient(config)
		if err != nil {
			fmt.Printf("demo-client terminated: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("unknown server type: %s\n", os.Args[2])
		os.Exit(1)
	}
}
