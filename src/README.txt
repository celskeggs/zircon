To generate twirp bindings:

 $ cd zircon/src/
 $ protoc --twirp_out=. --go_out=. ./zircon/rpc/twirp/*.proto

To generate mockery mocks:

 $ cd zircon/src/
 $ mockery -dir zircon/apis/ -name=Chunkserver -output zircon/apis/mocks/
 $ mockery -dir zircon/apis/ -name=Frontend -output zircon/apis/mocks/
 $ mockery -dir zircon/apis/ -name=MetadataCache -output zircon/apis/mocks/

To build binary:

 $ go build zircon/main/
