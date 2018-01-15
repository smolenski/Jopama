#!/bin/bash
#!/bin/bash
#
# ENV:
# ZOO_DIR
# LOG_DIR
# STORAGE_DIR
# PEERS
# EXTPORT
# MY_ID

function _runZooKeeper()
{
    if [[ $# -ne 6 ]]; then
        echo "${FUNCNAME[0]} arguments ($*)"
        return 1
    fi
    echo "args: $*"
    local zooDir=$1
    local logDir=$2
    local storageDir=$3
    local peers=( $(echo "$4" | sed -e "s:,: :g") )
    local extport=$5
    local myId=$6
    local zooConfigPathOrig=$zooDir/conf/zoo_sample.cfg
    local zooConfigPath=$zooDir/conf/zoo.cfg
    local zooServerExec=$zooDir/bin/zkServer.sh
    local myIdPath=$storageDir/myid
    if ! [[ -d $zooDir ]]; then
        echo "${FUNCNAME[0]} zooDir not found"
        return 1
    fi
    if ! [[ -d $logDir ]]; then
        echo "${FUNCNAME[0]} logDir not found"
        return 1
    fi
    if ! [[ -d $storageDir ]]; then
        echo "${FUNCNAME[0]} storageDir not found"
        return 1
    fi
    if [[ ${#peers[*]} -eq 0 ]]; then
        echo "${FUNCNAME[0]} peers not found"
        return 1
    fi
    if [[ -z $extport ]]; then
        echo "${FUNCNAME[0]} extport not found"
        return 1
    fi
    if [[ -z $myId ]]; then
        echo "${FUNCNAME[0]} myid not found"
        return 1
    fi
    if [[ -e $myIdPath ]]; then
        echo "${FUNCNAME[0]} file myid already exists"
        return 1
    fi
    if ! [[ -f $zooConfigPathOrig ]]; then
        echo "${FUNCNAME[0]} zooConfigPathOrig not found"
        return 1
    fi
    if ! [[ -f $zooServerExec ]]; then
        echo "${FUNCNAME[0]} zooServerExec not found"
        return 1
    fi
    cp $zooConfigPathOrig $zooConfigPath
    sed -i -e "s:dataDir=/tmp/zookeeper:dataDir=$storageDir:g" $zooConfigPath
    sed -i -e "s:clientPort=2181:clientPort=$extport:g" $zooConfigPath
    for ((i=0;i<${#peers[*]};++i)); do
       echo "server.$i=${peers[$i]}" >>$zooConfigPath
    done
    echo "$myId" >$myIdPath
    pushd $logDir
    #export JMXPORT=29800
    $zooServerExec "start"
    popd
    while true; do
        echo "+"
        sleep 5
    done
}

_runZooKeeper $*
