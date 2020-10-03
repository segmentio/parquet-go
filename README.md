## Test

`go test ./...`

To test against the parquet-tools reference implementation, make sure to have
`parquet-tools` in your `PATH`. [Build it][build] or [download it
pre-built][dl].

⚠️Buildkite does not test against parquet-tools because our base docker images
don't have both Go and Java available.

[build]: https://github.com/apache/parquet-mr/tree/master/parquet-tools
[dl]: https://github.com/pelletier/parquet-tools-bin

## References

* https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
* https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf

## Todo

- [x] `parquet-tools cat` reimplementation.
- [x] Use builder to remove allocs.
- [ ] RowReader projection.
- [ ] Predicates pushdown.
- [ ] Re-introduce Column interface.
- [ ] ReadSeeker => ReaderAt + size
- [ ] S3 partial reader.
- [ ] Benchmark with parquet-go.
- [ ] Writer.
- [ ] Out-of-band writer.
