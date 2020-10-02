#!/usr/bin/env bash
#emacs --eval '(ediff-files "/tmp/ptools.out" "/tmp/parquet-tools.out")'

set -ex

go build github.com/segmentio/parquet/cmd/ptools

time ./ptools cat "$@" > /tmp/ptools.out
time parquet-tools cat "$@" > /tmp/parquet-tools.out

diff /tmp/ptools.out /tmp/parquet-tools.out
