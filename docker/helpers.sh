#!/bin/bash

function runOnce()
{
    if [[ $# -ne 1 ]]; then
        echo "runOnce args"
        return 1
    fi
    local id=$1
    #python testRunner.py -dockerRunnerArg NATIVE:3 -numClusters 2 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
    #python testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 400 -outForTP 200 -duration 180
    #python testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 3 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 2 -outForTC 100 -outForTP 10 -duration 180
    python testRunner.py -dockerRunnerArg "DOCKERMACHINE:ens5;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 3 -numTP 3 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 2 -outForTC 100 -outForTP 10 -duration 180
    #python testRunner.py -dockerRunnerArg "DOCKERMACHINE:eth0;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -outForTC 100 -outForTP 20 -duration 180
    #python testRunner.py -dockerRunnerArg "DOCKERMACHINE:ens5;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 10 -outForTC 100 -outForTP 10 -duration 180
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
DM_DRIVER=kvm
DM_DRIVER=amazonec2

source ~/docker-machine-aws

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
    docker-machine ssh $name sudo mount -t ramfs -o size=1536M ramfs ${JOPAMA_DIR}
    docker-machine ssh $name sudo chmod 777 ${JOPAMA_DIR}
}

function setUpMachinesSaveLoad()
{
    local imgs=(smolenski/zookeeper smolenski/jopama)
    local archives=( $(for img in ${imgs[*]}; do echo ${img//\//_}.tar; done) )
    local numImages=${#imgs[*]}
    for ((i=0;i<$numImages;i++)); do
        docker save -o ${archives[$i]} ${imgs[$i]}
    done
    for ((i=0;i<${NUM_MACHINES};++i)); do
        setUpMachine $i
        local name=$(getMachineName $i)
        eval $(docker-machine env $name)
        for ((j=0;j<$numImages;j++)); do
            docker load -i ${archives[$j]}
        done
        eval $(docker-machine env -u)
    done
}

function setUpMachinesPull()
{
    local imgs=(smolenski/zookeeper smolenski/jopama)
    local numImages=${#imgs[*]}
    for ((i=0;i<${NUM_MACHINES};++i)); do
        setUpMachine $i
        local name=$(getMachineName $i)
        eval $(docker-machine env $name)
        for ((j=0;j<$numImages;j++)); do
            docker pull ${imgs[$j]}
        done
        eval $(docker-machine env -u)
    done
}

function createMachines()
{
    for ((i=0;i<${NUM_MACHINES};++i)); do
        docker-machine create --driver $DM_DRIVER $(getMachineName $i)
    done
}

function destroyMachines()
{
    for ((i=0;i<${NUM_MACHINES};++i)); do
        docker-machine stop $(getMachineName $i)
        docker-machine rm -f $(getMachineName $i)
    done
}