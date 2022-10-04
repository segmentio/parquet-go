AUTHORS.txt: .mailmap
	go install github.com/kevinburke/write_mailmap@latest
	write_mailmap > AUTHORS.txt

.PHONY: format
format:
	go install github.com/kevinburke/differ@latest
	differ gofmt -w .

GOTESTFLAGS = -race -v

.PHONY: coverage
coverage:
	go test $(GOTESTFLAGS) -coverpkg="./..." -coverprofile=.coverprofile ./...
	grep -v 'cmd' < .coverprofile > .covprof && mv .covprof .coverprofile
	go tool cover -func=.coverprofile
