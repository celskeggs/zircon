package rpc

import (
	"zircon/apis"
	"net"
	"net/http"
	"fmt"
	"context"
)

func LaunchEmbeddedHTTP(handler http.Handler, address apis.ServerAddress) (func(kill bool) error, apis.ServerAddress, error) {
	if address == "" {
		address = ":http"
	}

	listener, err := net.Listen("tcp", string(address))
	if err != nil {
		return nil, "", err
	}

	httpServer := &http.Server{Handler: handler}
	termErr := make(chan error)
	go func() {
		defer func() {
			err := recover()
			termErr <- fmt.Errorf("panic: %v", err)
		}()

		err := httpServer.Serve(listener)

		if err == http.ErrServerClosed {
			err = nil
		}
		termErr <- err
	}()

	teardown := func(kill bool) error {
		var err1 error
		if kill {
			err1 = httpServer.Shutdown(context.Background())
			if err1 == nil {
				err1 = listener.Close()
			}
		}
		err2 := <-termErr
		if err1 == nil {
			return err2
		} else if err2 == nil {
			return err1
		} else {
			return fmt.Errorf("multiple errors: { %v } and { %v }", err1, err2)
		}
	}

	return teardown, apis.ServerAddress(listener.Addr().String()), nil
}

func StringArrayToAddressArray(strings []string) []apis.ServerAddress {
	addresses := make([]apis.ServerAddress, len(strings))
	for i, v := range strings {
		addresses[i] = apis.ServerAddress(v)
	}
	return addresses
}

func AddressArrayToStringArray(addresses []apis.ServerAddress) []string {
	strings := make([]string, len(addresses))
	for i, v := range addresses {
		strings[i] = string(v)
	}
	return strings
}