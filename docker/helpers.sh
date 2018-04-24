#!/bin/bash

function runOnce()
{
    if [[ $# -ne 1 ]]; then
        echo "runOnce args"
        return 1
    fi
    local id=$1
    echo "Running: $id"
    #python testRunner.py -dockerRunnerArg NATIVE:3 -numClusters 2 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
    #python testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 400 -outForTP 200 -duration 180
    #python testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
    python testRunner.py -dockerRunnerArg "DOCKERMACHINE:eth0;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
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

NUM_MACHINES=1
JOPAMA_DIR=/var/jopamaTest

function getMachineName()
{
    if [[ $# -ne 1 ]]; then
        echo "arguments"
        return 1
    fi
    printf "myengine%03d" $1
}

function setUpMachine()
{
    if [[ $# -ne 1 ]]; then
        echo "arguments"
        return 1
    fi
    local id=$1
    local name=$(getMachineName $id)
    docker-machine ssh $name sudo mkdir ${JOPAMA_DIR}
    docker-machine ssh $name sudo mount -t ramfs -o size=300M ramfs ${JOPAMA_DIR}
    docker-machine ssh $name sudo chmod 777 ${JOPAMA_DIR}
}

function setUpMachines()
{
    imgs=(smolenski/zookeeper smolenski/jopama)
    archives=( $(for img in ${imgs[*]}; do echo ${img//\//_}.tar; done) )
    numImages=${#imgs[*]}
    for ((i=0;i<$numImages;i++)); do
        docker save -o ${archives[$i]} ${imgs[$i]}
    done
    for ((i=0;i<${NUM_MACHINES};++i)); do
        echo setUpMachine $i
        local name=$(getMachineName $i)
        eval $(docker-machine env $name)
        for ((j=0;j<$numImages;j++)); do
            docker load -i ${archives[$j]}
        done
        eval $(docker-machine env -u)
    done
}

function createMachines()
{
    for ((i=0;i<${NUM_MACHINES};++i)); do
        docker-machine create --driver kvm $(getMachineName $i) 
    done
}

function destroyMachines()
{
    for ((i=0;i<${NUM_MACHINES};++i)); do
        docker-machine stop $(getMachineName $i)
        docker-machine rm -f $(getMachineName $i)
    done
}
