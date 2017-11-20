#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "runJopama arguments"
    exit 1
fi

JOPAMA_DIR=$1
LOG_DIR=$2
DEBUG_PORT=$3
shift 3

source $JOPAMA_DIR/helpers.sh
runJopama $JOPAMA_DIR/Jopama/ $LOG_DIR $DEBUG_PORT $*
