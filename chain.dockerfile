# Considering that the glibc version in the conventional production environment is relatively low, ubuntu:18.04 is used
# ubuntu:18.04 -> glibc 2.27
# ubuntu:20.04 -> glibc 2.31
# ubuntu:22.04 -> glibc 2.34
FROM ubuntu:18.04

# Configure the go environment
ENV GOPATH=/go
ENV GOROOT=/usr/local/go
ENV PATH=${GOROOT}/bin:$PATH

ENV GO_VERSION=1.22.9

# Set environment variables to avoid interactive configuration prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# Make sure apt-get is up to date and dependent packages are installed
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Remove ol sources.list and configure azure mirrors
# RUN mv /etc/apt/sources.list /etc/apt/sources-bak.list
# RUN { \
#     echo 'deb http://azure.archive.ubuntu.com/ubuntu/ bionic main restricted universe multiverse'; \
#     echo 'deb http://azure.archive.ubuntu.com/ubuntu/ bionic-security main restricted universe multiverse'; \
#     echo 'deb http://azure.archive.ubuntu.com/ubuntu/ bionic-updates main restricted universe multiverse'; \
#     echo 'deb http://azure.archive.ubuntu.com/ubuntu/ bionic-proposed main restricted universe multiverse'; \
#     echo 'deb http://azure.archive.ubuntu.com/ubuntu/ bionic-backports main restricted universe multiverse'; \
# } > /etc/apt/sources.list

RUN apt-get update \
    && apt-get install -y tzdata build-essential curl ca-certificates xz-utils cpio wget zip unzip p7zip git mercurial bzr texinfo help2man \
    && ln -fs /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone \
    && dpkg-reconfigure --frontend noninteractive tzdata

# Deps update
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    gcc-6-arm-linux-gnueabi g++-6-arm-linux-gnueabi libc6-dev-armel-cross                \
    gcc-6-arm-linux-gnueabihf g++-6-arm-linux-gnueabihf libc6-dev-armhf-cross            \
    gcc-mingw-w64 g++-mingw-w64 \
    git \
    wget \
    curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* 

RUN \
  # Fix any stock package issues
  ln -s /usr/include/asm-generic /usr/include/asm && \
  # Fix git safe.directory
  git config --global --add safe.directory '*'

# aarch64
# https://releases.linaro.org/components/toolchain/binaries/?spm=5176.28103460.0.0.6a225d27ESgbyn
RUN curl -k -L -o /tmp/x86_64_aarch64_gcc_6.tar.xz https://releases.linaro.org/components/toolchain/binaries/6.5-2018.12/aarch64-linux-gnu/gcc-linaro-6.5.0-2018.12-x86_64_aarch64-linux-gnu.tar.xz && \
  tar -C /usr/local -xf /tmp/x86_64_aarch64_gcc_6.tar.xz && \
  rm -rf /tmp/x86_64_aarch64_gcc_6.tar.xz

ENV PATH=/usr/local/gcc-linaro-6.5.0-2018.12-x86_64_aarch64-linux-gnu/bin:$PATH

# Download golang
RUN wget -q https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm -rf go${GO_VERSION}.linux-amd64.tar.gz