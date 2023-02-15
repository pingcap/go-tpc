# Build the go-tpc binary
FROM golang:1.18 as builder

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the source
COPY . .

# Build
ARG TARGETARCH
RUN GOOS=linux GOARCH=$TARGETARCH make build

FROM ubuntu:22.04

COPY --from=builder /workspace/bin/go-tpc /go-tpc

ENTRYPOINT [ "/go-tpc" ]
