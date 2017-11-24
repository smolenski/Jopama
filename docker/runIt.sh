#!/bin/bash

function runOnce()
{
    if [[ $# -ne 1 ]]; then
        echo "runOnce args"
        return 1
    fi
    local id=$1
    echo "Running: $id"
    sudo rm -fr /var/jopamaTest/storage/ /var/jopamaTest/logs
    python testRunner.py -ips 127.0.0.1 -numClusters 1 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 1000 -compsInTra 10 -outForTC 10 -outForTP 5 -duration 300
}

function runNumTimes()
{
    if [[ $# -ne 1 ]]; then
        echo "runNumTimes args"
        return 1
    fi
    local numRuns=$1
    for ((i=0;i<${numRuns};i++)); do
        runOnce $i
        if [[ $? -ne 0 ]]; then
            echo "python FAILURE"
            return 1
        fi
    done
}
