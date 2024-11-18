#!/bin/bash

set -e

# Compiled Platform
platforms=("linux/amd64" "linux/arm64" "windows/amd64" "windows/386")

GO_VERSION_MAJOR=$(go version | sed -e 's/.*go\([0-9]\+\)\..*/\1/')
GO_VERSION_MINOR=$(go version | sed -e 's/.*go[0-9]\+\.\([0-9]\+\)\..*/\1/')

echo "Major golang version [$GO_VERSION_MAJOR] minor golang version [$GO_VERSION_MINOR]"

# Prepare build env
REPO="github.com/wentaojin/dbms"
COMMIT=$(git describe --always --no-match --tags --dirty="-dev")
BUILDTS=$(date '+%Y-%m-%d %H:%M:%S')
GITHASH=$(git rev-parse HEAD)
GITREF=$(git rev-parse --abbrev-ref HEAD)

LDFLAGS="-w -s"

# Add build flags
LDFLAGS+=" -X '$REPO/version.Version=$COMMIT'"
LDFLAGS+=" -X '$REPO/version.BuildTS=$BUILDTS'"
LDFLAGS+=" -X '$REPO/version.GitHash=$GITHASH'"
LDFLAGS+=" -X '$REPO/version.GitBranch=$GITREF'"

echo "Prepare build flags [$LDFLAGS]"

# Go main path
APP_SRC=$(pwd)
DBMS_CLI="$APP_SRC/component/cli/main.go"
DBMS_CLUSTER="$APP_SRC/component/cluster/main.go"
DBMS_MASTER="$APP_SRC/component/master/main.go"
DBMS_WORKER="$APP_SRC/component/worker/main.go"

# Compiled output
gocache="/gocache"
LINUX_AMD64_DIR="$gocache/linux/amd64"
LINUX_ARM64_DIR="$gocache/linux/arm64"
WINDOWS_AMD64_DIR="$gocache/windows/amd64"
WINDOWS_386_DIR="$gocache/windows/386"

mkdir -p $LINUX_AMD64_DIR
mkdir -p $LINUX_AMD64_DIR
mkdir -p $WINDOWS_AMD64_DIR
mkdir -p $WINDOWS_386_DIR

for platform in "${platforms[@]}"; do
    IFS='/' read -r XGOOS XGOARCH <<< "$platform"

    echo "Compiling for $XGOOS/$XGOARCH..."

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "linux" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "amd64" ]; }; then
        CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ GOOS=linux GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_AMD64_DIR/dbms-ctl" -ldflags "$LDFLAGS" ${DBMS_CLI}
        CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ GOOS=linux GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_AMD64_DIR/dbms-cluster" -ldflags "$LDFLAGS" ${DBMS_CLUSTER}
        CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ GOOS=linux GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_AMD64_DIR/dbms-master" -ldflags "$LDFLAGS" ${DBMS_MASTER}
        CC=x86_64-linux-gnu-gcc CXX=x86_64-linux-gnu-g++ GOOS=linux GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_AMD64_DIR/dbms-worker" -ldflags "$LDFLAGS" ${DBMS_WORKER}
    fi

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "linux" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "arm64" ]; }; then
        CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ GOOS=linux GOARCH=arm64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_ARM64_DIR/dbms-ctl" -ldflags "$LDFLAGS" ${DBMS_CLI}
        CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ GOOS=linux GOARCH=arm64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_ARM64_DIR/dbms-cluster" -ldflags "$LDFLAGS" ${DBMS_CLUSTER}
        CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ GOOS=linux GOARCH=arm64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_ARM64_DIR/dbms-master" -ldflags "$LDFLAGS" ${DBMS_MASTER}
        CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ GOOS=linux GOARCH=arm64 GO111MODULE=on CGO_ENABLED=1 go build -o "$LINUX_ARM64_DIR/dbms-worker" -ldflags "$LDFLAGS" ${DBMS_WORKER}
    fi

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "windows" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "amd64" ]; }; then
        CC=x86_64-w64-mingw32-gcc-posix CXX=x86_64-w64-mingw32-g++-posix GOOS=windows GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_AMD64_DIR/dbms-ctl.exe" -ldflags "$LDFLAGS" ${DBMS_CLI}
        CC=x86_64-w64-mingw32-gcc-posix CXX=x86_64-w64-mingw32-g++-posix GOOS=windows GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_AMD64_DIR/dbms-cluster.exe" -ldflags "$LDFLAGS" ${DBMS_CLUSTER}
        CC=x86_64-w64-mingw32-gcc-posix CXX=x86_64-w64-mingw32-g++-posix GOOS=windows GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_AMD64_DIR/dbms-master.exe" -ldflags "$LDFLAGS" ${DBMS_MASTER}
        CC=x86_64-w64-mingw32-gcc-posix CXX=x86_64-w64-mingw32-g++-posix GOOS=windows GOARCH=amd64 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_AMD64_DIR/dbms-worker.exe" -ldflags "$LDFLAGS" ${DBMS_WORKER}
    fi

    if { [ "$XGOOS" == "." ] || [ "$XGOOS" == "windows" ]; } && { [ "$XGOARCH" == "." ] || [ "$XGOARCH" == "386" ]; }; then
        CC=i686-w64-mingw32-gcc-posix CXX=i686-w64-mingw32-g++-posix GOOS=windows GOARCH=386 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_386_DIR/dbms-ctl.exe" -ldflags "$LDFLAGS" ${DBMS_CLI}
        CC=i686-w64-mingw32-gcc-posix CXX=i686-w64-mingw32-g++-posix GOOS=windows GOARCH=386 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_386_DIR/dbms-cluster.exe" -ldflags "$LDFLAGS" ${DBMS_CLUSTER}
        CC=i686-w64-mingw32-gcc-posix CXX=i686-w64-mingw32-g++-posix GOOS=windows GOARCH=386 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_386_DIR/dbms-master.exe" -ldflags "$LDFLAGS" ${DBMS_MASTER}
        CC=i686-w64-mingw32-gcc-posix CXX=i686-w64-mingw32-g++-posix GOOS=windows GOARCH=386 GO111MODULE=on CGO_ENABLED=1 go build -o "$WINDOWS_386_DIR/dbms-worker.exe" -ldflags "$LDFLAGS" ${DBMS_WORKER}
    fi
done