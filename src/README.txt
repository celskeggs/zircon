To generate twirp bindings:

 $ cd zircon/src/
 $ protoc --twirp_out=. --go_out=. ./zircon/rpc/twirp/*.proto

To build binary:

 $ go build zircon/main/
