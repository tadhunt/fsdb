all:
	go mod tidy
	go vet
	go build

clean:
	go clean -modcache
	go mod tidy
