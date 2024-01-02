.PHONY: build runMaster stopMaster runWorker stopWorker clean gotool

MASTERCMD="./cmd/master.go"
WORKERCMD="./cmd/worker.go"
CTLCMD="./ctl/dbmsctl.go"
BINARYPATH="bin/"
CONFIGPATH="./example"

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
LDFLAGS += -X "$(REPO)/config.Version=$(COMMIT)"
LDFLAGS += -X "$(REPO)/config.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(REPO)/config.GitHash=$(GITHASH)"
LDFLAGS += -X "$(REPO)/config.GitBranch=$(GITREF)"


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