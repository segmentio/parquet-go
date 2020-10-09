parquet.version=2.8.0

.PHONY:
gen: internal/gen-go/parquet/parquet.go

internal/gen-go/parquet/parquet.go: Dockerfile.thrift
	docker build --build-arg parquet=${parquet.version} -f Dockerfile.thrift --tag parquet-thrift .
	docker run -v "${PWD}:/out" -u $$(id -u) parquet-thrift thrift -o /out --gen go /parquet.thrift
	rm -rf internal/gen-go
	mv gen-go internal/

buildkite:
	docker build . -f Dockerfile.buildkite -t parquet-buildkite
	docker run -ti parquet-buildkite go test ./...
