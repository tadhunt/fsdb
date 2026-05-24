include ./secrets/config.mk

all:
	go mod tidy
	go vet
	staticcheck
	go build

clean:
	go clean -modcache
	go mod tidy

# `make test` runs unit tests plus integration tests against a Firestore
# emulator. run-with-emulator.sh starts the emulator, exports
# FIRESTORE_EMULATOR_HOST, and execs the given command.
test: all
	./run-with-emulator.sh go test -v -count=1 ./...
