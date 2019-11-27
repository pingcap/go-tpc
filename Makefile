GOOS := $(if $(GOOS),$(GOOS),linux)
GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) GO111MODULE=on go

PACKAGE_LIST  := go list ./...| grep -vE "cmd"
PACKAGES  := $$($(PACKAGE_LIST))
FILES     := $$(find . -name "*.go" | grep -vE "vendor")
GOFILTER := grep -vE 'vendor|render.Delims|bindata_assetfs|testutil|\.pb\.go'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

all: build

fmt:
	@echo "gofmt"
	@gofmt -s -l -w $(FILES) 2>&1 | $(GOCHECKER)

test:
	go test ./... -cover $(PACKAGES)

build:
	go build -o ./bin/go-tpc cmd/go-tpc/*