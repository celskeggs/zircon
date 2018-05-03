package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
	"zircon/chunkserver"
	"zircon/chunkserver/control"
	"zircon/chunkserver/storage"
	"zircon/client"
	"zircon/client/demo"
	"zircon/rpc"
	"zircon/apis"
)

type Config struct {
	Address     apis.ServerAddress
	StorageType string
	StoragePath string

	ClientConfig client.Configuration
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

	finish, _, err := rpc.PublishChunkserver(server, config.Address)
	if err != nil {
		return err
	}
	return finish(false) // wait for server to finish
}

func LaunchFrontend(config *Config) error {
	panic("unimplemented")
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
		fmt.Printf(" - frontend\n")
		fmt.Printf(" - demo-client\n")
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
	case "demo-client":
		err := LaunchDemoClient(config)
		if err != nil {
			fmt.Printf("chunkserver terminated: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("unknown server type: %s\n", os.Args[2])
		os.Exit(1)
	}
}
