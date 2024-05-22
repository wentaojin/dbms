.PHONY: build runMaster stopMaster runWorker stopWorker clean gotool

MASTERCMD="./component/master/main.go"
WORKERCMD="./component/worker/main.go"
CTLCMD="./component/cli/main.go"
CLUSTERCMD="./component/cluster/main.go"
BINARYPATH="bin/"
CONFIGPATH="./sample"

MASTERCONFIG00TEMP = $(CONFIGPATH)/master_config00_temp.toml
MASTERCONFIG01TEMP = $(CONFIGPATH)/master_config01_temp.toml
MASTERCONFIG02TEMP = $(CONFIGPATH)/master_config02_temp.toml

WORKERCONFIG00TEMP = $(CONFIGPATH)/worker_config00_temp.toml
WORKERCONFIG01TEMP = $(CONFIGPATH)/worker_config01_temp.toml
WORKERCONFIG02TEMP = $(CONFIGPATH)/worker_config02_temp.toml

SEEDIPVALUE = 192.168.0.101

MASTERCONFIG00 = $(CONFIGPATH)/master_config00.toml
MASTERCONFIG01 = $(CONFIGPATH)/master_config01.toml
MASTERCONFIG02 = $(CONFIGPATH)/master_config02.toml

WORKERCONFIG00 = $(CONFIGPATH)/worker_config00.toml
WORKERCONFIG01 = $(CONFIGPATH)/worker_config01.toml
WORKERCONFIG02 = $(CONFIGPATH)/worker_config02.toml

REPO    := github.com/wentaojin/dbms

GOOS    := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH  := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
GOENV   := GO111MODULE=on CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH)
GO      := $(GOENV) go
GOBUILD := $(GO) build
GORUN   := $(GO) run
SHELL   := /usr/bin/env bash

COMMIT  := $(shell git describe --always --no-match --tags --dirty="-dev")
BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITREF  := $(shell git rev-parse --abbrev-ref HEAD)

CURRENTIPADDR := $(shell ipconfig getifaddr en0)

LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/version.Version=$(COMMIT)"
LDFLAGS += -X "$(REPO)/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REPO)/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REPO)/version.GitBranch=$(GITREF)"


runMaster: clean gotool seedMaster masterProgram00 masterProgram01 masterProgram02

stopMaster:
	ps -ef| grep 'example/master' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	rm -f $(MASTERCONFIG00) $(MASTERCONFIG01) $(MASTERCONFIG02)

runWorker: clean gotool seedWorker workerProgram00 workerProgram01 workerProgram02

stopWorker:
	ps -ef| grep 'example/worker' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	rm -f $(WORKERCONFIG00) $(WORKERCONFIG01) $(WORKERCONFIG02)

masterProgram00:
	$(GORUN) $(MASTERCMD) --config $(MASTERCONFIG00) &

masterProgram01:
	$(GORUN) $(MASTERCMD) --config $(MASTERCONFIG01) &

masterProgram02:
	$(GORUN) $(MASTERCMD) --config $(MASTERCONFIG02) &

workerProgram00:
	$(GORUN) $(WORKERCMD) --config $(WORKERCONFIG00) &

workerProgram01:
	$(GORUN) $(WORKERCMD) --config $(WORKERCONFIG01) &

workerProgram02:
	$(GORUN) $(WORKERCMD) --config $(WORKERCONFIG02) &

seedMaster:
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG00TEMP) > $(MASTERCONFIG00)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG01TEMP) > $(MASTERCONFIG01)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG02TEMP) > $(MASTERCONFIG02)

seedWorker:
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG00TEMP) > $(WORKERCONFIG00)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG01TEMP) > $(WORKERCONFIG01)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG02TEMP) > $(WORKERCONFIG02)

build: clean gotool
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-master $(MASTERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-worker $(WORKERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbmsctl $(CTLCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms $(CLUSTERCMD)

gotool:
	$(GO) mod tidy

clean:
	@if [ -f ${BINARYPATH} ] ; then rm ${BINARYPATH} ; fi

help:
	@echo "make - Format Go code and compile it into a binary file"
	@echo "make build - compile Go code and generate binaries"
	@echo "make runMaster - run Go Master code directly"
	@echo "make stopMaster - Stop the Go Master code directly"
	@echo "make runWorker - run Go Worker code directly"
	@echo "make stopWorker - Stop Go Worker code directly"
	@echo "make clean - remove binaries and vim swap files"
	@echo "make gotool - Run the Go tool 'mod tidy'"