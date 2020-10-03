FROM thrift

ARG parquet

RUN apt-get update -y && apt-get install -y curl
RUN curl -L https://github.com/apache/parquet-format/archive/apache-parquet-format-${parquet}.tar.gz | tar -xzvf - parquet-format-apache-parquet-format-${parquet}/src/main/thrift/parquet.thrift -O > /parquet.thrift