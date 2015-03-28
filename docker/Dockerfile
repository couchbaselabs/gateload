FROM tleyden5iwx/ubuntu-go

MAINTAINER Traun Leyden <tleyden@couchbase.com>

RUN go get -u -v -t github.com/couchbaselabs/gateload

RUN cd /opt/go/src/github.com/couchbaselabs/gateload && \
    git checkout feature/debug && \
    go build && \
    go install