#!/bin/bash

function runOnce()
{
    if [[ $# -ne 1 ]]; then
        echo "runOnce args"
        return 1
    fi
    local id=$1
    #python -u testRunner.py -dockerRunnerArg NATIVE:3 -numClusters 2 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -sclForTC 3 -sclForTP 3 -outForTC 100 -outForTP 20 -duration 180
    #python -u testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -sclForTC 3 -sclForTP 3 -outForTC 400 -outForTP 200 -duration 180
    #python -u testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 3 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 2 -sclForTC 3 -sclForTP 3 -outForTC 100 -outForTP 10 -duration 180
    python -u testRunner.py -dockerRunnerArg NATIVE:1 -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 1 -compsInTra 1 -sclForTC 100 -sclForTP 1 -outForTC 100 -outForTP 10 -duration 180
    #python -u testRunner.py -dockerRunnerArg "DOCKERMACHINE:ens5;myengine000,myengine001,myengine002" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 3 -numTP 3 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 4 -sclForTC 3 -sclForTP 3 -outForTC 400 -outForTP 20 -duration 180
    #python -u testRunner.py -dockerRunnerArg "DOCKERMACHINE:eth0;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 1 -numTP 1 -numTC 1 -firstComp 100 -numComp 10000 -compsInTra 10 -sclForTC 3 -sclForTP 3 -outForTC 100 -outForTP 20 -duration 180
    #python -u testRunner.py -dockerRunnerArg "DOCKERMACHINE:ens5;myengine000" -outputDir /tmp/jopamaResults -numClusters 1 -clusterSize 3 -numTP 1 -numTC 1 -firstComp 100 -numComp 100000 -compsInTra 10 -sclForTC 3 -sclForTP 3 -outForTC 100 -outForTP 10 -duration 180
    local retVal=$?
    echo "python finished with $retVal"
    return $retVal
}

function getMachinesString()
{
    if [[ $# -ne 1 ]]; then
        echo "getMachinesString args"
        return 1
    fi
    local mult=$1
    local str=""
    for ((n=0;n<3*mult;++n)); do
        if [[ $n -gt 0 ]]; then
            str="${str},"
        fi
        str="${str}$(getMachineName $n)"
    done
    echo $str
}

NUM_MACHINES=3
export JOPAMA_DIR=/var/jopamaTest
export DM_DRIVER=kvm
export DM_DRIVER=amazonec2

source ~/docker-machine-aws

function performTestForMult()
{
    if [[ $# -ne 0 ]]; then
        echo "performTestForMult args"
        return 1
    fi
    local mult=$((NUM_MACHINES / 3))
    local mstr="$(getMachinesString $mult)"
    local numClusters=$mult
    if [[ -z ${NUM_COMPS} ]]; then
        echo "performTestForMult: NUM_COMPS not set"
        return 1
    fi
    if [[ -z ${COMPS_IN_TRANS} ]]; then
        echo "performTestForMult: COMPS_IN_TRANS not set"
        return 1
    fi
    if [[ -z ${SCL_FOR_TP} ]]; then
        echo "performTestForMult: SCL_FOR_TP not set"
        return 1
    fi
    { python -u testRunner.py -dockerRunnerArg "DOCKERMACHINE:ens5;${mstr}" -outputDir /tmp/jopamaResults -numClusters $numClusters -clusterSize 3 -numTP 3 -numTC 1 -firstComp 100 -numComp ${NUM_COMPS} -compsInTra ${COMPS_IN_TRANS} -sclForTC $((2 * 512 * ${COMPS_IN_TRANS} / ${NUM_COMPS})) -sclForTP ${SCL_FOR_TP} -outForTC 512 -outForTP 40 -duration 180 2>&1; } | tee ~/dockerLogs/testRunner_$(date +%Y%m%d_%H%M%S).log
}

function performTestForSimple()
{
    if [[ $# -ne 0 ]]; then
        echo "performTestForMult args"
        return 1
    fi
    local numClusters=1
    local clusterSize=1
    if [[ -z ${NUM_COMPS} ]]; then
        echo "performTestForMult: NUM_COMPS not set"
        return 1
    fi
    if [[ -z ${COMPS_IN_TRANS} ]]; then
        echo "performTestForMult: COMPS_IN_TRANS not set"
        return 1
    fi
    if [[ -z ${SCL_FOR_TP} ]]; then
        echo "performTestForMult: SCL_FOR_TP not set"
        return 1
    fi
    { python -u testRunner.py -dockerRunnerArg "NATIVE:$((numClusters * clusterSize))" -outputDir /tmp/jopamaResults -alllogs -numClusters ${numClusters} -clusterSize ${clusterSize} -numTP ${clusterSize} -numTC 1 -firstComp 100 -numComp ${NUM_COMPS} -compsInTra ${COMPS_IN_TRANS} -sclForTC $((2 * 512 * ${COMPS_IN_TRANS} / ${NUM_COMPS})) -sclForTP ${SCL_FOR_TP} -outForTC 512 -outForTP 40 -duration 180 2>&1; } | tee ~/dockerLogs/testRunner_$(date +%Y%m%d_%H%M%S).log
}

function performSeqTests
{
    local sbdir=/home/barbara/zoo/zookeeper-3.4.9/code/Jopama/docker/perfSeqTestingResultsX00
    local stdir=""
    local mult=$((NUM_MACHINES / 3))
    local multStr=$(printf "%02d" ${mult})
    local lastCreated=""
    #for numComps in 1 2 4 16 64 256 1024 16384; do
    #for numComps in 1 256 4096 32768; do
    for compsInTrans in 2 16; do
        for numComps in 16 4096 131072; do
            for sclForTP in 1; do
                export COMPS_IN_TRANS=${compsInTrans}
                export NUM_COMPS=${numComps} 
                export SCL_FOR_TP=${sclForTP}
                stdir=${sbdir}/nm${NUM_MACHINES}_comps${numComps}/cint_${compsInTrans}_scltp_${sclForTP}
                mkdir -p $stdir
                performTestForMult
                #performTestForSimple
                lastCreated=$(ls -1c ~/dockerLogs/ | head -1)
                mv ~/dockerLogs/${lastCreated} $stdir
                mv /tmp/jopamaResults $stdir
                rm -fr /tmp/jopamaResults
            done
        done
    done
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

function myParallel()
{
    parallel --jobs 12 $*
}

function getMachineIDs()
{
    seq 0 $((NUM_MACHINES - 1))
}

function getMachineName()
{
    if [[ $# -ne 1 ]]; then
        echo "arguments"
        return 1
    fi
    printf "myengine%03d" $1
}

export -f getMachineName

function myDockerMachine()
{
    /home/barbara/docker/docker-machine $*
}

export -f myDockerMachine

function _setUpMachine()
{
    if [[ $# -ne 1 ]]; then
        echo "arguments"
        return 1
    fi
    local id=$1
    local name=$(getMachineName $id)
    myDockerMachine ssh $name sudo mkdir ${JOPAMA_DIR}
    myDockerMachine ssh $name sudo mount -t ramfs -o size=2048M ramfs ${JOPAMA_DIR}
    myDockerMachine ssh $name sudo chmod 777 ${JOPAMA_DIR}
}

export -f _setUpMachine

function setUpMachineSaveLoad()
{
    local id=$1
    local name=$(getMachineName $id)
    local archives=( $(echo $2) )
    _setUpMachine $id
    eval $(myDockerMachine env $name)
    for ((j=0;j<${#archives[*]};j++)); do
        docker load -i ${archives[$j]}
    done
    eval $(myDockerMachine env -u)
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
        setUpMachineSaveLoad $i "${archives[*]}"
    done
}

function _setUpMachinePull()
{
    local id=$1
    local name=$(getMachineName $id)
    local imgs=(smolenski/zookeeper smolenski/jopama)
    local numImages=${#imgs[*]}
    _setUpMachine $id
    eval $(myDockerMachine env $name)
    for ((j=0;j<$numImages;j++)); do
        docker pull ${imgs[$j]}
    done
    eval $(myDockerMachine env -u)
}

export -f _setUpMachinePull

function setUpMachinesPull()
{
    getMachineIDs | myParallel _setUpMachinePull {1}
}

function _createMachine()
{
    local id=$1
    local name=$(getMachineName $id)
    myDockerMachine create --driver $DM_DRIVER --amazonec2-request-spot-instance --amazonec2-spot-price 0.1 $name
}

export -f _createMachine

function createMachines()
{
    getMachineIDs | myParallel _createMachine {1}
}

function _destroyMachine()
{
    local id=$1
    local name=$(getMachineName $id)
    myDockerMachine stop $name
    myDockerMachine rm -f $name
}

export -f _destroyMachine

function destroyMachines()
{
    getMachineIDs | myParallel _destroyMachine {1}
}
