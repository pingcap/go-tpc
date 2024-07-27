# Build the go-tpc binary
FROM golang:1.21 as builder

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the source
COPY . .

# Build
ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH make build

FROM alpine

RUN apk add --no-cache \
  dumb-init \
  tzdata \
  # help to setup or teardown database schemas
  mariadb-client

COPY --from=builder /workspace/bin/go-tpc /go-tpc

ENTRYPOINT [ "/usr/bin/dumb-init" ]
CMD ["/go-tpc"]
