#!/bin/sh

case $(uname -s) in
    Linux|linux) os=linux ;;
    Darwin|darwin) os=darwin ;;
    *) os= ;;
esac

if [ -z "$os" ]; then
    echo "OS $(uname -s) not supported." >&2
    exit 1
fi

case $(uname -m) in
    amd64|x86_64) arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) arch= ;;
esac

if [ -z "$arch" ]; then
    echo "Architecture  $(uname -m) not supported." >&2
    exit 1
fi

check_depends() {
    pass=0
    command -v tar >/dev/null || {
        echo "Dependency check failed: please install 'tar' before proceeding."
        pass=1
    }
    return $pass
}

if ! check_depends; then
    exit 1
fi

bin_dir=$(cd $(dirname $0) && pwd)

install_binary() {
  ls $bin_dir/dbms-cluster-*.tar.gz | xargs -n1 tar zxvf || return 1
  ls $bin_dir/dbms-ctl-*.tar.gz | xargs -n1 tar zxvf || return 1
  return 0
}

if ! install_binary; then
    echo "Failed to download and/or extract dbms-cluster and dbms-ctl archive."
    exit 1
fi

bold=$(tput bold 2>/dev/null)
sgr0=$(tput sgr0 2>/dev/null)

# Reference: https://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux-unix
shell=$(echo $SHELL | awk 'BEGIN {FS="/";} { print $NF }')
echo "Detected shell: ${bold}$shell${sgr0}"
if [ -f "${HOME}/.${shell}_profile" ]; then
    PROFILE=${HOME}/.${shell}_profile
elif [ -f "${HOME}/.${shell}_login" ]; then
    PROFILE=${HOME}/.${shell}_login
elif [ -f "${HOME}/.${shell}rc" ]; then
    PROFILE=${HOME}/.${shell}rc
else
    PROFILE=${HOME}/.profile
fi
echo "Shell profile:  ${bold}$PROFILE${sgr0}"

case :$PATH: in
    *:$bin_dir:*) : "PATH already contains $bin_dir" ;;
    *) printf 'export PATH=%s:$PATH\n' "$bin_dir" >> "$PROFILE"
        echo "$PROFILE has been modified to to add dbms-cluster to PATH"
        echo "open a new terminal or ${bold}source ${PROFILE}${sgr0} to use it"
        ;;
esac

echo "Installed path: ${bold}$bin_dir/dbms${sgr0}"
echo "==============================================="
echo "${bold}source ${PROFILE}${sgr0}"
echo "==============================================="