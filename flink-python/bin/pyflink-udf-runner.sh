#!/usr/bin/env bash

python=${python:-python}

if [[ "$FLINK_TESTING" = "1" ]]; then
    ACTUAL_FLINK_HOME=`cd $FLINK_HOME; pwd -P`
    FLINK_SOURCE_ROOT_DIR=`cd $ACTUAL_FLINK_HOME/../../../../; pwd`
    FLINK_PYTHON="${FLINK_SOURCE_ROOT_DIR}/flink-python"
    if [[ -f "${FLINK_PYTHON}/pyflink/fn_execution/boot.py" ]]; then
        # use pyflink source code to override the pyflink.zip in PYTHONPATH
        # to ensure loading latest code
        export PYTHONPATH="$FLINK_PYTHON:$PYTHONPATH"
    fi
fi

if [[ "$_PYTHON_WORKING_DIR" != "" ]]; then
    # set current working directory to $_PYTHON_WORKING_DIR
    cd "$_PYTHON_WORKING_DIR"
    if [[ "$python" == ${_PYTHON_WORKING_DIR}* ]]; then
        # The file extracted from archives may not preserve its original permission.
        # Set minimum execution permission to prevent from permission denied error.
        chmod +x "$python"
    fi
fi

log="$BOOT_LOG_DIR/flink-python-udf-boot.log"
${python} -m pyflink.fn_execution.boot $@ 2>&1 | tee ${log}
