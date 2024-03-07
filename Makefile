GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOARCH=$(GOARCH) GO111MODULE=on go

PACKAGE_LIST  := go list ./...| grep -vE "cmd"
PACKAGES  := $$($(PACKAGE_LIST))
FILES_TO_FMT  := $(shell find . -path -prune -o -name '*.go' -print)

LDFLAGS += -X "main.version=$(shell git describe --tags --dirty --always)"
LDFLAGS += -X "main.commit=$(shell git rev-parse HEAD)"
LDFLAGS += -X "main.date=$(shell date -u '+%Y-%m-%d %I:%M:%S')"

GOBUILD=$(GO) build -ldflags '$(LDFLAGS)'

# Image URL to use all building/pushing image targets
IMG ?= go-tpc:latest
PLATFORM ?= linux/amd64,linux/arm64

all: format test build

format: vet fmt

fmt:
	@echo "gofmt"
	@gofmt -w ${FILES_TO_FMT}
	@git diff --exit-code .

test:
	go test ./... -cover $(PACKAGES)

build: mod
	$(GOBUILD) -o ./bin/go-tpc cmd/go-tpc/*

vet:
	go vet ./...

mod:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	@git diff --exit-code -- go.sum go.mod

docker-build: test
	docker build . -t ${IMG}

docker-push: docker-build
	docker push ${IMG}

# Create multiarch driver if not exists:
#   docker buildx create --name multiarch --driver docker-container --use
docker-multiarch: test
	docker buildx build --platform ${PLATFORM} . -t ${IMG} --push
