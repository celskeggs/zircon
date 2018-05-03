#!/bin/bash
set -e -u

cd "$(dirname "$0")"

# generate twirp bindings

echo "Generating twirp bindings"
protoc --twirp_out=. --go_out=. ./zircon/rpc/twirp/*.proto

# generate mockery mocks

for interface in Chunkserver Frontend MetadataCache
do
	mockery -dir zircon/apis/ "-name=${interface}" -output zircon/apis/mocks/
done

# build binary

echo "building binary..."

go build zircon/main

# run tests

echo "launching tests..."

TESTDIRS=$((for x in $(find zircon -name '*_test.go'); do dirname $x; done) | sort -u)
FINALTESTS=

for testdir in ${TESTDIRS}
do
	if [ ! -e "${testdir}/no-auto-test" ]
	then
		FINALTESTS+="${testdir} "
	else
		echo "SKIPPED TESTS IN DIRECTORY: ${testdir}"
	fi
done

go test ${FINALTESTS}

echo "built!"
