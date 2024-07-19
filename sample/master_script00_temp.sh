#!/bin/bash

# Change to the directory of this script
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Script masterServer00 current directory is: $scriptDir"

parentDir="$( cd "$( dirname "$scriptDir" )" &> /dev/null && pwd )"
echo "Script masterServer00 parent directory is: $parentDir"

binPath="$parentDir/bin"
configPath="$parentDir/sample"
masterConfig="$configPath/master_config00.toml"

# get os kernel name
KERNEL=$(uname -s)

case $KERNEL in
Linux) 	echo "Detecting operating system: Linux";;
Darwin) echo "Detecting operating system:  Darwin (macOS)";;
  *)    echo "Unsupported operating system: $KERNEL"; exit 1;;
esac

if [ "$KERNEL" == "Linux" ]; then
  export LD_LIBRARY_PATH=LIBRARY_DIR_VAR
  echo "Setting  masterServer00 script LD_LIBRARY_PATH to: $LIBRARY_PATH_VAR"
elif [ "$KERNEL" == "Darwin" ]; then
  export DYLD_LIBRARY_PATH=LIBRARY_DIR_VAR
  echo "Setting  masterServer00 script DYLD_LIBRARY_PATH to: $LIBRARY_PATH_VAR"
fi

echo "Starting masterServer00 script Command [$binPath/dbms-master --config $masterConfig]..."
"$binPath"/dbms-master --config "$masterConfig"