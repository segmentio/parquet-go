* https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
* https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf

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
