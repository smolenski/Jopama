#!/bin/bash

function runOnce()
{
    if [[ $# -ne 1 ]]; then
        echo "runOnce args"
        return 1
    fi
    local id=$1
    echo "Running: $id"
    #python testRunner.py -dockerRunnerArg NATIVE: -numHosts 3 -numClusters 2 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
    python testRunner.py -dockerRunnerArg NATIVE: -outputDir /tmp/jopamaResults -numHosts 1 -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
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
