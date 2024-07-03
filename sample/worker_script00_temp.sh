#!/usr/bin/bash

BINPATH="./bin"
CONFIGPATH="./sample"
WORKERCONFIG="${CONFIGPATH}/worker_config00.toml"

# get os kernel name
KERNEL=$(uname -s)

case $KERNEL in
Linux) 	echo "Detecting operating system: Linux";;
Darwin) echo "Detecting operating system:  Darwin (macOS)";;
  *)    echo "Unsupported operating system: $KERNEL"; exit 1;;
esac

if [ "$KERNEL" == "Linux" ]; then
  export LD_LIBRARY_PATH=LIBRARY_DIR_VAR
  echo "Setting  workerServer00 script LD_LIBRARY_PATH to: $LIBRARY_PATH_VAR"
elif [ "$KERNEL" == "Darwin" ]; then
  export DYLD_LIBRARY_PATH=LIBRARY_DIR_VAR
  echo "Setting  workerServer00 script DYLD_LIBRARY_PATH to: $LIBRARY_PATH_VAR"
fi

echo "Starting workerServer00 script Command [${BINPATH}/dbms-worker --config ${WORKERCONFIG}]..."
${BINPATH}/dbms-worker --config ${WORKERCONFIG}