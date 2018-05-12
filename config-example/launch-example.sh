#!/bin/bash
set -e -u

echo "requires fuse to be installed, along with etcd and a compiled version of zircon, stored in the 'main' file"

mkdir -p data-cs0 data-cs1

fusermount -u zircon0 || true
fusermount -u zircon1 || true

mkdir -p zircon0 zircon1

./etcd &
./main cs0.yaml chunkserver &
./main cs1.yaml chunkserver &
./main mdc.yaml metadata-cache &
./main frontend.yaml frontend &
./main sync.yaml sync-server &
./main fuse.yaml fuse &
./main fuse1.yaml fuse &

sleep 1

echo "launched services; look at zircon0 and zircon1 directories"
