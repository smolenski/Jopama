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
    python testRunner.py -ips 127.0.0.1 -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 100 -compsInTra 2 -outForTC 10 -outForTP 2 -duration 30
    local retVal=$?
    echo "python finished with $retVal"
    return $retVal
}

function runNumTimes()
{
    if [[ $# -ne 1 ]]; then
        echo "runNumTimes args"
        return 1
    fi
    local numRuns=$1
    local retVal=0
    for ((i=0;i<${numRuns};i++)); do
        { runOnce $i 2>&1; } | tee ~/dockerLogs/testRunner_$(date +%s).log
        retVal=$?
        if [[ $retVal -ne 0 ]]; then
            echo "python FAILURE $retVal"
            return 1
        fi
    done
}
