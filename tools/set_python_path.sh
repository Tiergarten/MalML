#/bin/bash

INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set PYSPARK_SUBMIT_ARGS="--name" "PySparkShell" "pyspark-shell --master local[8]"

if [[ $(uname) != "Darwin" ]]; then
    SET_PATH=$(readlink -f "${INSTALL_DIR}/..")
    echo "setting PYTHONPATH -> ${SET_PATH}"
    export PYTHONPATH=$(cygpath -w "${SET_PATH}")
else
    export PYTHONPATH="${INSTALL_DIR}/.."
fi