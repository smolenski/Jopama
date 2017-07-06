#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "runJopama arguments"
    exit 1
fi

JOPAMA_DIR=$1
LOG_DIR=$2
shift 2

source $JOPAMA_DIR/helpers.sh
runJopama $JOPAMA_DIR/Jopama/ $LOG_DIR $*
