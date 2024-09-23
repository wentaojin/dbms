.ONESHELL: build runMaster stopMaster runWorker stopWorker clean gotool

PWD:=$(shell pwd)
MASTERCMD = $(PWD)/component/master/main.go
WORKERCMD = $(PWD)/component/worker/main.go
CTLCMD = $(PWD)/component/cli/main.go
CLUSTERCMD = $(PWD)/component/cluster/main.go
BINARYPATH = $(PWD)/bin
CONFIGPATH = $(PWD)/sample

MASTERCONFIG00TEMP = $(CONFIGPATH)/master_config00_temp.toml
MASTERCONFIG01TEMP = $(CONFIGPATH)/master_config01_temp.toml
MASTERCONFIG02TEMP = $(CONFIGPATH)/master_config02_temp.toml

MASTERSCRIPT00TEMP = $(CONFIGPATH)/master_script00_temp.sh
MASTERSCRIPT01TEMP = $(CONFIGPATH)/master_script01_temp.sh
MASTERSCRIPT02TEMP = $(CONFIGPATH)/master_script02_temp.sh

WORKERCONFIG00TEMP = $(CONFIGPATH)/worker_config00_temp.toml
WORKERCONFIG01TEMP = $(CONFIGPATH)/worker_config01_temp.toml
WORKERCONFIG02TEMP = $(CONFIGPATH)/worker_config02_temp.toml

WORKERSCRIPT00TEMP = $(CONFIGPATH)/worker_script00_temp.sh
WORKERSCRIPT01TEMP = $(CONFIGPATH)/worker_script01_temp.sh
WORKERSCRIPT02TEMP = $(CONFIGPATH)/worker_script02_temp.sh

SEEDIPVALUE = 192.168.0.101
SEEDLIBRARY = LIBRARY_DIR_VAR

MASTERCONFIG00 = $(CONFIGPATH)/master_config00.toml
MASTERCONFIG01 = $(CONFIGPATH)/master_config01.toml
MASTERCONFIG02 = $(CONFIGPATH)/master_config02.toml

MASTERSCRIPT00 = $(CONFIGPATH)/master_script00.sh
MASTERSCRIPT01 = $(CONFIGPATH)/master_script01.sh
MASTERSCRIPT02 = $(CONFIGPATH)/master_script02.sh

MASTERSCRIPTLOG00 = $(CONFIGPATH)/master_script00.log
MASTERSCRIPTLOG01 = $(CONFIGPATH)/master_script01.log
MASTERSCRIPTLOG02 = $(CONFIGPATH)/master_script02.log

WORKERCONFIG00 = $(CONFIGPATH)/worker_config00.toml
WORKERCONFIG01 = $(CONFIGPATH)/worker_config01.toml
WORKERCONFIG02 = $(CONFIGPATH)/worker_config02.toml

WORKERSCRIPT00 = $(CONFIGPATH)/worker_script00.sh
WORKERSCRIPT01 = $(CONFIGPATH)/worker_script01.sh
WORKERSCRIPT02 = $(CONFIGPATH)/worker_script02.sh

WORKERSCRIPTLOG00 = $(CONFIGPATH)/worker_script00.log
WORKERSCRIPTLOG01 = $(CONFIGPATH)/worker_script01.log
WORKERSCRIPTLOG02 = $(CONFIGPATH)/worker_script02.log

REPO    := github.com/wentaojin/dbms

GOOS    := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH  := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
GOENV   := GO111MODULE=on CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH)
GO      := $(GOENV) go
GOBUILD := $(GO) build
GORUN   := $(GO) run

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

OS := $(shell uname -s)

LIBRARY_DIR ?=

# LIBRARY_PATH_VAR the default value
LIBRARY_PATH_VAR ?= /Users/wentaojin/storehouse/oracle/instantclient_19_8

ifeq ($(strip $(LIBRARY_DIR)),)
    # If LIBRARY_DIR is empty, print a warning message
    $(warning "The command line flag [LIBRARY_DIR] is not set. Using the default directory [$(LIBRARY_PATH_VAR)].")
endif

ifeq ($(strip $(LIBRARY_DIR)),)
	export LIBRARY_PATH_VAR := $(LIBRARY_PATH_VAR)
else
	export LIBRARY_PATH_VAR := $(LIBRARY_DIR)
endif

runMaster: gotool buildMaster seedMaster masterServer00 masterServer01 masterServer02

stopMaster: clean
	ps -ef| grep 'master_script*' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	ps -ef| grep 'dbms-master' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	rm -f $(MASTERCONFIG00) $(MASTERCONFIG01) $(MASTERCONFIG02)
	rm -f $(MASTERSCRIPT00) $(MASTERSCRIPT01) $(MASTERSCRIPT02)
	rm -f $(MASTERSCRIPTLOG00) $(MASTERSCRIPTLOG01) $(MASTERSCRIPTLOG02)

runWorker: gotool buildWorker seedWorker workerServer00 workerServer01 workerServer02

stopWorker: clean
	ps -ef| grep 'worker_script*' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	ps -ef| grep 'dbms-worker' | grep -v 'grep' | awk '{print $$2}' | xargs kill -9
	rm -f $(WORKERCONFIG00) $(WORKERCONFIG01) $(WORKERCONFIG02)
	rm -f $(WORKERSCRIPT00) $(WORKERSCRIPT01) $(WORKERSCRIPT02)
	rm -f $(WORKERSCRIPTLOG00) $(WORKERSCRIPTLOG01) $(WORKERSCRIPTLOG02)

masterServer00:
	@echo "Setting  masterServer00 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting masterServer00 script..."
	@nohup sh $(MASTERSCRIPT00) > $(MASTERSCRIPTLOG00) 2>&1 &
	@echo "Starting masterServer00 background has started..."
masterServer01:
	@echo "Setting  masterServer01 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting masterServer01 script..."
	@nohup sh $(MASTERSCRIPT01) > $(MASTERSCRIPTLOG01) 2>&1 &
	@echo "Starting masterServer01 background has started..."
masterServer02:
	@echo "Setting  masterServer02 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting masterServer02 script..."
	@nohup sh $(MASTERSCRIPT02) > $(MASTERSCRIPTLOG02) 2>&1 &
	@echo "Starting masterServer02 background has started..."

workerServer00:
	@echo "Setting  workerServer00 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting workerServer00 script..."
	@nohup sh $(WORKERSCRIPT00) > $(WORKERSCRIPTLOG00) 2>&1 &
	@echo "Starting workerServer00 background has started..."
workerServer01:
	@echo "Setting  workerServer01 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting workerServer01 script..."
	@nohup sh $(WORKERSCRIPT01) > $(WORKERSCRIPTLOG01) 2>&1 &
	@echo "Starting workerServer01 background has started..."
workerServer02:
	@echo "Setting  workerServer02 script LIBRARY_DIR_VAR to: $$LIBRARY_PATH_VAR"
	@echo "Starting workerServer02 script..."
	@nohup sh $(WORKERSCRIPT02) > $(WORKERSCRIPTLOG02) 2>&1 &
	@echo "Starting workerServer01 background has started..."

seedMaster:
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG00TEMP) > $(MASTERCONFIG00)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG01TEMP) > $(MASTERCONFIG01)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(MASTERCONFIG02TEMP) > $(MASTERCONFIG02)

	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(MASTERSCRIPT00TEMP) > $(MASTERSCRIPT00)
	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(MASTERSCRIPT01TEMP) > $(MASTERSCRIPT01)
	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(MASTERSCRIPT02TEMP) > $(MASTERSCRIPT02)

	chmod +x $(MASTERSCRIPT00)
	chmod +x $(MASTERSCRIPT01)
	chmod +x $(MASTERSCRIPT02)

seedWorker:
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG00TEMP) > $(WORKERCONFIG00)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG01TEMP) > $(WORKERCONFIG01)
	sed 's/$(SEEDIPVALUE)/$(CURRENTIPADDR)/g' $(WORKERCONFIG02TEMP) > $(WORKERCONFIG02)

	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(WORKERSCRIPT00TEMP) > $(WORKERSCRIPT00)
	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(WORKERSCRIPT01TEMP) > $(WORKERSCRIPT01)
	sed 's|$(SEEDLIBRARY)|$(LIBRARY_PATH_VAR)|g' $(WORKERSCRIPT02TEMP) > $(WORKERSCRIPT02)

	chmod +x $(WORKERSCRIPT00)
	chmod +x $(WORKERSCRIPT01)
	chmod +x $(WORKERSCRIPT02)

buildMaster:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-master $(MASTERCMD)

buildWorker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-worker $(WORKERCMD)

build: clean gotool
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-master $(MASTERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-worker $(WORKERCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-ctl $(CTLCMD)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o $(BINARYPATH)/dbms-cluster $(CLUSTERCMD)

gotool:
	$(GO) mod tidy

clean:
	@if [ -d ${BINARYPATH} ] ; then rm -rf ${BINARYPATH} ; fi

help:
	@echo "make - Format Go code and compile it into a binary file"
	@echo "make build - compile Go code and generate binaries"
	@echo "make runMaster - run Go Master code directly"
	@echo "make stopMaster - Stop the Go Master code directly"
	@echo "make runWorker - run Go Worker code directly"
	@echo "make stopWorker - Stop Go Worker code directly"
	@echo "make clean - remove binaries and vim swap files"
	@echo "make gotool - Run the Go tool 'mod tidy'"