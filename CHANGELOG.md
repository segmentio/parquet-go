 # v0.17.0 / preview

 ## Breaking Changes

 - [#3] migrate to module github.com/parquet-go/parquet-go @kevinburke 
 - [#16] drop support for `go1.17` @gernest

 ## Bug fixes

 - [#18] fix error handling when reading from io.ReaderAt @gernest
 - [#9] fix zero value of nested field point @gernest
 - [#31] fix memory corruption in `MergeRowGroups` @gernest

 ## Enhancements
 - [#17] performance improvement on GenericReader @gernest, @zolstein
 - [#11] stabilize flakey `TestOpenFile` @gernest
