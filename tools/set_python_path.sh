INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SET_PATH=$(readlink -f "${INSTALL_DIR}/..")
echo "setting PYTHONPATH -> ${SET_PATH}"
export PYTHONPATH=$(cygpath -w "${SET_PATH}")
set PYSPARK_SUBMIT_ARGS="--name" "PySparkShell" "pyspark-shell"