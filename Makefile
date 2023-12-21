.PHONY: build runMaster stopMaster runWorker stopWorker clean gotool

MASTERCMD="./cmd/master.go"
WORKERCMD="./cmd/worker.go"
CTLCMD="./ctl/dbmsctl.go"
BINARYPATH="bin/"
CONFIGPATH="./example/"

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


LDFLAGS := -w -s
LDFLAGS += -X "$(REPO)/config.Version=$(COMMIT)"
LDFLAGS += -X "$(REPO)/config.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REPO)/config.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REPO)/config.GitBranch=$(GITREF)"


runMaster: clean gotool masterProgram00 masterProgram01 masterProgram02

stopMaster:
	kill -9 $$(cat $(MASTERCMD)/masterProgram00.pid) 2>/dev/null || true
	kill -9 $$(cat $(MASTERCMD)/masterProgram01.pid) 2>/dev/null || true
	kill -9 $$(cat $(MASTERCMD)/masterProgram02.pid) 2>/dev/null || true
	rm -f $(MASTERCMD)/masterProgram00.pid $(MASTERCMD)/masterProgram01.pid $(MASTERCMD)/masterProgram02.pid

runWorker: clean gotool workerProgram00 workerProgram01 workerProgram02

stopWorker:
	-kill -9 $$(cat $(MASTERCMD)/workerProgram00.pid) 2>/dev/null || true
	-kill -9 $$(cat $(MASTERCMD)/workerProgram01.pid) 2>/dev/null || true
	-kill -9 $$(cat $(MASTERCMD)/workerProgram02.pid) 2>/dev/null || true
	rm -f $(MASTERCMD)/workerProgram00.pid $(MASTERCMD)/workerProgram01.pid $(MASTERCMD)/workerProgram02.pid

masterProgram00:
	$(GORUN) $(MASTERCMD) --config $(CONFIGPATH)/master_config00.toml & echo $$! > $(MASTERCMD)/masterProgram00.pid

masterProgram01:
	$(GORUN) $(MASTERCMD) --config $(CONFIGPATH)/master_config01.toml & echo $$! > $(MASTERCMD)/masterProgram01.pid

masterProgram02:
	$(GORUN) $(MASTERCMD) --config $(CONFIGPATH)/master_config02.toml & echo $$! > $(MASTERCMD)/masterProgram02.pid

workerProgram00:
	$(GORUN) $(WORKERCMD) --config $(CONFIGPATH)/worker_config00.toml & echo $$! > $(WORKERCMD)/workerProgram00.pid

workerProgram01:
	$(GORUN) $(WORKERCMD) --config $(CONFIGPATH)/worker_config01.toml & echo $$! > $(WORKERCMD)/workerProgram01.pid

workerProgram02:
	$(GORUN) $(WORKERCMD) --config $(CONFIGPATH)/worker_config02.toml & echo $$! > $(WORKERCMD)/workerProgram02.pid

build: clean gotool
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-master $(MASTERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-worker $(WORKERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbmsctl $(CTLCMD)

gotool:
	$(GO) mod tidy

clean:
	@if [ -f ${BINARYPATH} ] ; then rm ${BINARYPATH} ; fi

help:
	@echo "make - 格式化 Go 代码, 并编译生成二进制文件"
	@echo "make build - 编译 Go 代码, 生成二进制文件"
	@echo "make runMaster - 直接运行 Go Master 代码"
	@echo "make stopMaster - 直接停止 Go Master 代码"
	@echo "make runWorker - 直接运行 Go Worker 代码"
	@echo "make stopWorker - 直接停止 Go Worker 代码"
	@echo "make clean - 移除二进制文件和 vim swap files"
	@echo "make gotool - 运行 Go 工具 'mod tidy'"