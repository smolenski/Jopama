import argparse
import doctest
import sys
import subprocess
import time
import os
import random
from datetime import datetime

def getDockerRunnerTypeAndConfigStr(dockerRunnerArg):
    """
    >>> getDockerRunnerTypeAndConfigStr('drType:drConfig')
    ('drType', 'drConfig')
    """
    idx = dockerRunnerArg.find(":")
    assert idx != -1
    drType = dockerRunnerArg[:idx]
    drConfig = dockerRunnerArg[idx + 1:]
    return (drType, drConfig)

class DockerRunner(object):
    def __init__(self, configStr, args):
        self._configStr = configStr
        self._args = args

    """
    Prepare environment
    Returns list of hostIds
    """
    def prepare(self):
        raise NotImplementedError

    """
    Get ips for hosts (ordered - with respect to hostId)
    """
    def getHostIps(self):
        raise NotImplementedError

    """
    Run process on hostId
    Returns processId
    """
    def runDockerCmd(self, hostId, cmd): 
        raise NotImplementedError

    """
    Cleanup environment
    """
    def cleanup(self):
        raise NotImplementedError

class NativeDockerRunner(DockerRunner):
    def __init__(self, configStr, args):
        super(NativeDockerRunner, self).__init__(configStr, args)

    def prepare(self):
        self.cleanup()
        subprocess.check_call('rm -fr /var/jopamaTest/*', shell=True)
        for gId in range(self._args.numClusters * self._args.clusterSize):
            zkStorageDir=format("/var/jopamaTest/storage/%d/ZOOKEEPER" % gId)
            zkLogsDir=format("/var/jopamaTest/logs/%d/ZOOKEEPER" % gId)
            tpLogsDir=format("/var/jopamaTest/logs/%d/TP" % gId)
            tcLogsDir=format("/var/jopamaTest/logs/%d/TC" % gId)
            for dirToCreate in [zkStorageDir, zkLogsDir, tpLogsDir, tcLogsDir]:
                subprocess.check_call('mkdir -p %s' % dirToCreate, shell=True)
        ccLogsDir=format("/var/jopamaTest/logs/CC")
        tvLogsDir=format("/var/jopamaTest/logs/TV")
        for dirToCreate in [ccLogsDir, tvLogsDir]:
            subprocess.check_call('mkdir -p %s' % dirToCreate, shell=True)

    def getHostIps(self):
        return ['127.0.0.1'] * self._args.numHosts

    def runDockerCmd(self, hostId, cmd):
        return subprocess.check_output(cmd, shell=True)

    def cleanup(self):
        subprocess.check_call("docker ps -aq | while read line; do docker kill $line; docker rm $line; done", shell=True)
        subprocess.check_call("rm -fr /var/jopamaTest/*", shell=True)

class ScopedDockerRunnerWrapper(object):
    def __init__(self, dockerRunner):
        self._dockerRunner = dockerRunner

    def __enter__(self):
        self._dockerRunner.prepare()
        return self._dockerRunner

    def __exit__(self, excType, excVal, excTb):
        self._dockerRunner.cleanup()
        

def createDockerRunner(args):
    drType, drConfig = getDockerRunnerTypeAndConfigStr(args.dockerRunnerArg) 
    if drType == "NATIVE":
        return NativeDockerRunner(drConfig, args)
    else:
        raise NotImplementedError

def getZooServerName(gId):
    return format('%d_ZOO_SERVER' % gId)

def getTCName(gId):
    return format('%d_TC' % gId)

def getTPName(gId):
    return format('%d_TP' % gId)

def getZooClientName():
    return format('ZOO_CLIENT_%s' % str(random.getrandbits(100)))

def id2ZKPortExt(id):
    return 10000 + 10 * id

def id2ZKPortInt1(id):
    return 10001 + 10 * id

def id2ZKPortInt2(id):
    return 10002 + 10 * id

def id2TPDebugPort(id):
    return 10003 + 10 * id

def id2TCDebugPort(id):
    return 10004 + 10 * id

def printIt(cmd, shell=None):
    print('%s' % cmd)

#subprocess.check_call=printIt

class Instance(object):
    def __init__(self, gId, clId, coId, hostId, runTP, runTC):
        self.gId = gId
        self.clId = clId
        self.coId = coId
        self.hostId = hostId
        self.runTP = runTP
        self.runTC = runTC
    def __str__(self):
        return "gId: %d clId: %d coId: %d hostId: %s runTP: %s runTC: %s" % (self.gId, self.clId, self.coId, self.hostId, self.runTP, self.runTC)

class ConfigGenerator(object):
    def __init__(self, args):
        self.args=args
        self.dist=[]
    def generate(self):
        numTPOnHostId = [0] * args.numHosts
        numTCOnHostId = [0] * args.numHosts
        for cid in range(args.numClusters):
            gIdAndNumP=[]
            gIdAndNumC=[]
            for rid in range(args.clusterSize):
                gId = cid * args.clusterSize + rid
                hostId = gId % args.numHosts
                gIdAndNumP.append((gId, numTPOnHostId[hostId]))
                gIdAndNumC.append((gId, numTCOnHostId[hostId]))
            tpss=[a[0] for a in sorted(gIdAndNumP, key=lambda x: x[1])][:args.numTP]
            tcss=[a[0] for a in sorted(gIdAndNumC, key=lambda x: x[1])][:args.numTC]
            for rid in range(args.clusterSize):
                gId = cid * args.clusterSize + rid
                hostId = gId % args.numHosts
                if gId in tpss:
                    numTPOnHostId[hostId] = numTPOnHostId[hostId] + 1
                if gId in tcss:
                    numTCOnHostId[hostId] = numTCOnHostId[hostId] + 1
                inst = Instance(gId, cid, rid, hostId, gId in tpss, gId in tcss)
                self.dist.append(inst)
        assert max(numTPOnHostId) - min(numTPOnHostId) <= 1
        assert max(numTCOnHostId) - min(numTCOnHostId) <= 1
    def getZKServersConf(self, gId, hostIps = None):
        assert hostIps is not None
        startId=gId - (gId % self.args.clusterSize)
        stopId=startId + self.args.clusterSize
        assert len(self.dist) >= stopId
        servers=[]
        for mId in range(startId, stopId):
            intPort1=None
            intPort2=None
            intPort1=id2ZKPortInt1(mId)
            intPort2=id2ZKPortInt2(mId)
            mInst = self.dist[mId]
            servers.append(hostIps[mInst.hostId] + ':' + str(intPort1) + ':' + str(intPort2))
        return ','.join(servers)
    def getZKServerId(self, gId):
        return gId % self.args.clusterSize 

class TestRunner(object):
    def __init__(self, args, gen, dockerRunner):
        self.args = args
        self.gen = gen
        self.dist = self.gen.dist
        self.dockerRunner = dockerRunner 
        self.start = None
        self.finish = None

    def getAddresses(self):
        hostIps = self.dockerRunner.getHostIps()
        clId2Member={}
        for i in range(self.args.numClusters):
            clId2Member[i]=[]
        for ins in self.dist:
            clId2Member[ins.clId].append(ins)
        addresses=[]
        for clId in sorted(clId2Member):
            for ins in clId2Member[clId]:
                addresses.append(format('%s:%d' % (hostIps[ins.hostId], id2ZKPortExt(ins.gId))))
        return ','.join(addresses)

    def startZooKeepers(self):
        for ins in self.dist:
            name=getZooServerName(ins.gId)
            hostStorageDir=format("/var/jopamaTest/storage/%d/ZOOKEEPER" % ins.gId)
            hostLogsDir=format("/var/jopamaTest/logs/%d/ZOOKEEPER" % ins.gId)
            extPort=id2ZKPortExt(ins.gId)
            intPort1=id2ZKPortInt1(ins.gId)
            intPort2=id2ZKPortInt2(ins.gId)
            peers=self.gen.getZKServersConf(ins.gId, self.dockerRunner.getHostIps())
            zkId=self.gen.getZKServerId(ins.gId)
            print("Starting ZooKeeper, ins: %d, extport: %d" % (ins.gId, extPort))
            self.dockerRunner.runDockerCmd(
                hostId = ins.hostId,
                cmd = 'docker run -d --name %s --net host -v %s:/var/jopamaTest/storage -v %s:/var/jopamaTest/logs -p %d -p %d -p %d smolenski/zookeeper %s %d %d'
                %
                (
                    name,
                    hostStorageDir,
                    hostLogsDir,
                    extPort,
                    intPort1,
                    intPort2,
                    peers,
                    extPort,
                    zkId
                )
            )

    def zkCli(self, server, zkCliCmd):
        name=getZooClientName()
        assert self.args.numHosts > 0
        output=self.dockerRunner.runDockerCmd(
            hostId = 0,
            cmd = 'docker run --name %s --network host --entrypoint /opt/ZooKeeper/zookeeper-3.4.9/bin/zkCli.sh smolenski/zookeeper -server %s %s'
            %
            (
                name,
                server,
                zkCliCmd
            ),
        )
        return output

    def startTPs(self):
        for ins in self.dist:
            if ins.runTP:
                print("Starting TP %s" % (ins.gId))
                name=getTPName(ins.gId)
                hostLogsDir=format("/var/jopamaTest/logs/%d/TP" % ins.gId)
                self.dockerRunner.runDockerCmd(
                    hostId = ins.hostId,
                    cmd = 'docker run -d --name %s --net host -v %s:/var/jopamaTest/logs smolenski/jopama %d pl.rodia.jopama.integration.zookeeper.ZooKeeperTransactionProcessorRunner %s %s %d %s %d %d'
                    %
                    (
                        name,
                        hostLogsDir,
                        id2TPDebugPort(ins.gId),
                        name,
                        self.getAddresses(),
                        self.args.clusterSize,
                        '/StartFinish',
                        ins.clId,
                        self.args.outForTP
                    ),
                )

    def startTCs(self):
        for ins in self.dist:
            if ins.runTC:
                print("Starting TC %s" % (ins.gId))
                name=getTCName(ins.gId)
                hostLogsDir=format("/var/jopamaTest/logs/%d/TC" % ins.gId)
                self.dockerRunner.runDockerCmd(
                    hostId = ins.hostId,
                    cmd = 'docker run -d --name %s --net host -v %s:/var/jopamaTest/logs smolenski/jopama %d pl.rodia.jopama.integration.zookeeper.ZooKeeperTransactionCreatorRunner %s %s %d %s %d %d %d %d %d'
                    %
                    (
                        name,
                        hostLogsDir,
                        id2TCDebugPort(ins.gId),
                        name,
                        self.getAddresses(),
                        self.args.clusterSize,
                        '/StartFinish',
                        ins.clId, 
                        self.args.outForTC,
                        self.args.firstComp,
                        self.args.numComp,
                        self.args.compsInTra
                    ),
                )

    def createComponents(self):
        print("Running CC")
        name='CC'
        hostLogsDir=format("/var/jopamaTest/logs/%s" % name)
        addresses=self.getAddresses()
        print("Starting CC")
        assert self.args.numHosts > 0
        self.dockerRunner.runDockerCmd(
            hostId = 0,
            cmd = 'docker run --name %s --net host -v %s:/var/jopamaTest/logs smolenski/jopama 25000 pl.rodia.jopama.integration.zookeeper.ZooKeeperComponentCreator CC %s %d %d %d'
            %
            (
                name,
                hostLogsDir,
                addresses,
                self.args.clusterSize,
                self.args.firstComp,
                self.args.numComp    
            ),
        )

    def verifyComponents(self):
        print("Running TV")
        name='TV'
        hostLogsDir=format("/var/jopamaTest/logs/%s" % name)
        addresses=self.getAddresses()
        print("Starting TV")
        assert self.args.numHosts > 0
        self.dockerRunner.runDockerCmd(
            hostId = 0,
            cmd = 'docker run --name %s --net host -v %s:/var/jopamaTest/logs smolenski/jopama 25000 pl.rodia.jopama.integration.zookeeper.ZooKeeperTestVerifier TV %s %d %d %d'
            %
            (
                name,
                hostLogsDir,
                addresses,
                self.args.clusterSize,
                self.args.firstComp,
                self.args.numComp
            ),
            shell=True
        )

    def waitForFiles(self, clusterId, path, numFiles):
        print("Waiting for files, clusterId: %d, path: %s, numFiles: %d" % (clusterId, path, numFiles))
        while True:
            print("Waiting ...")
            files = self.performLs(clusterId, path)
            if len(files) == numFiles:
                break
            time.sleep(1)
        print("Waiting for files: done")

    def waitForReady(self):
        print("Waiting for ready")
        self.waitForFiles(0, '/TP_READY', self.args.numTP * self.args.numClusters)
        self.waitForFiles(0, '/TC_READY', self.args.numTC * self.args.numClusters)

    def waitForDone(self):
        print("Waiting for done")
        self.waitForFiles(0, '/TC_DONE', self.args.numTC * self.args.numClusters)
        self.waitForFiles(0, '/TP_DONE', self.args.numTP * self.args.numClusters)

    def triggerStart(self):
        print("Triggering start")
        self.zkCli(self.getConnStrForCluster(0), 'create /StartFinish/START a')

    def triggerFinish(self):
        print("Triggering finish")
        self.zkCli(self.getConnStrForCluster(0), 'create /StartFinish/FINISH a')

    def waitDesiredDuration(self):
        print("Wait desired duration")
        time.sleep(self.args.duration)

    def getConnStrForCluster(self, clusterId):
        clusters=[]
        for i in range(self.args.numClusters):
            clusters.append([])
        for ins in self.dist:
           clusters[ins.clId].append(ins) 
        assert len(clusters) > 0
        cluster=clusters[clusterId]
        assert len(cluster) > 0
        ins=cluster[0]
        return format('%s:%d' % (self.dockerRunner.getHostIps()[ins.hostId], id2ZKPortExt(ins.gId)))

    def performLs(self, clusterId, path):
        output=self.zkCli(self.getConnStrForCluster(clusterId), 'ls %s' % (path))
        lines=output.splitlines()
        assert len(lines) > 0
        line=lines[-1]
        print('ls output: %s' % line)
        assert len(line) > 1
        if (line == '[]'):
            return []
        else:
            return line[1:-1].replace(',', '').split()

    def listFiles(self):
        print('Addresses: %s' % (self.getAddresses()))
        print('Performing ls')
        for i in range(self.args.numClusters):
            print('Listing dirs/files for cluster: %d' % i)
            for path in ['/', '/Components', '/Transactions']:
                files=self.performLs(i, path)
                print('%s => %s' % (path, str(files)))
        for path in ['/TC_READY', '/TP_READY']:
            files=self.performLs(0, path)
            print('%s => %s' % (path, str(files)))

    def prepareDirectories(self):
        time.sleep(5)
        for i in range(self.args.numClusters):
            connStr=self.getConnStrForCluster(i)
            self.zkCli(connStr, 'create /Transactions a')
            self.zkCli(connStr, 'create /Components a')
        connStr=self.getConnStrForCluster(0)
        self.zkCli(connStr, 'create /StartFinish a')
        self.zkCli(connStr, 'create /TP_READY a')
        self.zkCli(connStr, 'create /TP_DONE a')
        self.zkCli(connStr, 'create /TC_READY a')
        self.zkCli(connStr, 'create /TC_DONE a')

    def getContent(self, clusterId, path):
        print('getContent clusterId: %d path: %s' % (clusterId, path))
        output=self.zkCli(self.getConnStrForCluster(clusterId), 'get %s' % (path))
        lines=output.splitlines()
        assert len(lines) > 0
        line=lines[-1]
        print('getContent output: %s' % line)
        return line

    def getProcessedTransactionsNum(self):
        files=self.performLs(0, '/TC_DONE')
        created={}
        for name in files:
            path = format('/TC_DONE/%s' % (name))
            doneStr = self.getContent(0, path)
            tokens = doneStr.split()
            assert len(tokens) == 2
            created[name]=int(tokens[1])
        print('Created: %s' % str(created)) 
        numCreated=0
        for key in created: 
            numCreated+=created[key]
        print('NumCreated: %d' % numCreated)
        existing={}
        for clusterId in range(self.args.numClusters):
            files=self.performLs(clusterId, '/Transactions')
            existing[clusterId]=len(files)
        print('Existing: %s' % str(existing)) 
        numExisting=0
        for key in existing: 
            numExisting+=existing[key]
        print('NumExisting: %d' % numExisting)
        assert numCreated >= numExisting
        numProcessed = numCreated - numExisting
        print('NumProcessed: %d' % numProcessed)
        return numProcessed

    def printResult(self):
        durSec = (self.finish - self.start).seconds
        num = self.getProcessedTransactionsNum()
        print("Performance num: %d dur: %d speed: %f (T/s)" % (num, durSec, float(num) / durSec))
        
    def run(self):
        self.startZooKeepers()
        self.prepareDirectories()
        self.createComponents()
        self.startTPs()
        self.startTCs()
        self.waitForReady()
        self.start = datetime.now()
        self.triggerStart()
        self.waitDesiredDuration()
        #self.listFiles()
        self.triggerFinish()
        self.waitForDone()
        self.finish = datetime.now()
        self.verifyComponents()
        self.printResult()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate jopama docker cluster configuration')
    parser.add_argument('-dockerRunnerArg', type=str, required=True, dest='dockerRunnerArg')
    parser.add_argument('-numHosts', type=int, required=True, dest='numHosts')
    parser.add_argument('-numClusters', type=int, required=True, dest='numClusters')
    parser.add_argument('-clusterSize', type=int, required=True, dest='clusterSize')
    parser.add_argument('-numTP', type=int, required=True, dest='numTP', help='Num transaction processors per cluster')
    parser.add_argument('-numTC', type=int, required=True, dest='numTC', help='Num transaction creators per cluster')
    parser.add_argument('-firstComp', type=int, required=True, dest='firstComp', help="First component")
    parser.add_argument('-numComp', type=int, required=True, dest='numComp', help="Number of components")
    parser.add_argument('-compsInTra', type=int, required=True, dest='compsInTra', help="Components in transaction")
    parser.add_argument('-outForTC', type=int, required=True, dest='outForTC', help="Outstanding transactions for creator")
    parser.add_argument('-outForTP', type=int, required=True, dest='outForTP', help="Outstanding transactions for processor")
    parser.add_argument('-duration', type=int, required=True, dest='duration', help="Test duration")
    parser.add_argument('-doctest', action='store_true', dest='doctest', help="Run doctests")
    args = parser.parse_args()
    if args.doctest:
        doctest.testmod()
        sys.exit(0) 
    if (args.numClusters * args.clusterSize) % args.numHosts != 0:
        print('Arguments, numClusters * clusterSize should be multiplication of numHosts, numClusters: %d, clusterSize: %d numHosts: %d' % (args.numClusters, args.clusterSize, args.numHosts))
        sys.exit(1)
    assert args.numTP > 0
    assert args.numTP <= args.clusterSize
    assert args.numTC > 0
    assert args.numTC <= args.clusterSize
    print("args: %s" % args)
    gen = ConfigGenerator(args)
    gen.generate()
    print('DIST BEGIN')
    for inst in gen.dist:
        print('%s' % str(inst))
    print('DIST END')
    with ScopedDockerRunnerWrapper(createDockerRunner(args)) as dockerRunner:
        test = TestRunner(args, gen, dockerRunner)
        test.run()
    sys.exit(0)
