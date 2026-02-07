include ./secrets/config.mk

all:
	go mod tidy
	go vet
	staticcheck
	go build

clean:
	go clean -modcache
	go mod tidy

test: all
	go test -v -count=1 ./...
