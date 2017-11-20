#!/bin/bash

function prepareDist()
{
    if [[ $# -ne 3 ]]; then
        echo "${FUNCNAME[0]} arguments (destDir, zooDir, jopamaDir)"
        return 1
    fi
    local projectName=Jopama
    local archName=${projectName}.tar.gz
    local destDir=$1
    local distBaseDir=/tmp/distDir_${RANDOM}
    local distDir=$distBaseDir/$projectName
    local buildArch=${distBaseDir}/$archName
    local destFile=$destDir/$archName
    local distLibDir=${distDir}/lib
    local zooDir=$2
    local jopamaDir=$3
    if [[ -a $destFile ]]; then
        echo "dest file $destFile already exist"
        return 1
    fi
    if ! [[ -d $destDir ]]; then
        echo "dir $destDir does not exist"
        return 1
    fi
    if [[ -d $distBaseDir ]]; then
        echo "dir $distDir already exists"
        return 1
    fi
    mkdir -p $distDir
    mkdir $distLibDir
    ant -f $jopamaDir/build.xml jar
    if [[ $? -ne 0 ]]; then echo "ant failed"; return 1; fi
    cp $(find $jopamaDir -name Jopama.jar) $distDir
    cp $zooDir/zookeeper*.jar $distLibDir
    cp $zooDir/lib/*.jar $distLibDir
    cp $jopamaDir/lib/* $distLibDir
    cp $jopamaDir/log4j2.xml $distDir
    tar -czf $buildArch -C $distBaseDir $projectName
    cp $buildArch $destFile
    rm -fr $distBaseDir
}

function runJopama()
{
    if [[ $# -lt 3 ]]; then
        echo "${FUNCNAME[0]} arguments (distDir runDir debugPort javaArgs)"
        return 1
    fi
    local distDir=$1
    local runDir=$2
    local debugPort=$3
    shift 3
    if ! [[ -d $distDir ]]; then
        echo "${FUNCNAME[0]} dist dir $distDir does not exist"
        return 1
    fi
    if ! [[ -d $runDir ]]; then
        echo "${FUNCNAME[0]} run dir $runDir does not exist"
        return 1
    fi
    pushd $runDir
    set -x
    java -agentlib:jdwp=transport=dt_socket,address=${debugPort},server=y,suspend=n -ea -classpath "$distDir:$distDir/*:$distDir/lib/*" $*
    set +x
    popd
}
