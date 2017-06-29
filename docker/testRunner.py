import argparse
import doctest
import sys
import subprocess
import time
from datetime import datetime

def id2ZKPort(id):
    return 10000 + 3 * id

def id2TPPort(id):
    return 10001 + 3 * id

def id2TCPort(id):
    return 10002 + 3 * id

class Instance(object):
    def __init__(self, gId, clId, coId, ip, runTP, runTC):
        self.gId = gId
        self.clId = clId
        self.coId = coId
        self.ip = ip
        self.runTP = runTP
        self.runTC = runTC
    def __str__(self):
        return "gId: %d clId: %d coId: %d ip: %s runTP: %s runTC: %s" % (self.gId, self.clId, self.coId, self.ip, self.runTP, self.runTC)

class ConfigGenerator(object):
    def __init__(self, args):
        self.args=args
        self.dist=[]
    def generate(self):
        numTPOnIp={}
        numTCOnIp={}
        for ip in args.ips:
            numTPOnIp[ip]=0
            numTCOnIp[ip]=0    
        for cid in range(args.numClusters):
            gIdAndNumP=[]
            gIdAndNumC=[]
            for rid in range(args.clusterSize):
                gId = cid * args.clusterSize + rid
                ip = args.ips[gId % len(args.ips)]
                gIdAndNumP.append((gId, numTPOnIp[ip]))
                gIdAndNumC.append((gId, numTCOnIp[ip]))
            tpss=[a[0] for a in sorted(gIdAndNumP, key=lambda x: x[1])][:args.numTP]
            tcss=[a[0] for a in sorted(gIdAndNumC, key=lambda x: x[1])][:args.numTC]
            for rid in range(args.clusterSize):
                gId = cid * args.clusterSize + rid
                ip = args.ips[gId % len(args.ips)]
                if gId in tpss:
                    numTPOnIp[ip] = numTPOnIp[ip] + 1
                if gId in tcss:
                    numTCOnIp[ip] = numTCOnIp[ip] + 1
                inst = Instance(gId, cid, rid, ip, gId in tpss, gId in tcss)
                self.dist.append(inst)
        assert max(numTPOnIp.values()) - min(numTPOnIp.values()) <= 1
        assert max(numTCOnIp.values()) - min(numTCOnIp.values()) <= 1

class TestRunner(object):
    def __init__(self, args, dist):
        self.args = args
        self.dist = dist
        self.start = None
        self.finish = None

    def rotateLogDir(self):
        print("Rotating log directory (not impl yet)")

    def startZooKeepers(self):
        for instance in self.dist:
            print("Starting ZK %s:%d" % (instance.ip, id2ZKPort(instance.gId)))

    def startTPs(self):
        for instance in self.dist:
            if instance.runTP:
                print("Starting TP %s:%d" % (instance.ip, id2TPPort(instance.gId)))
        
    def startTCs(self):
        for instance in self.dist:
            if instance.runTC:
                print("Starting TC %s:%d" % (instance.ip, id2TCPort(instance.gId)))

    def runCC(self):
        print("Running CC")

    def runTV(self):
        print("Running TV")

    def waitForReady(self):
        print("Waiting for ready")

    def waitForDone(self):
        print("Waiting for done")

    def triggerStart(self):
        print("Triggering start")

    def triggerFinish(self):
        print("Triggering finish")

    def waitDesiredDuration(self):
        print("Wait desired duration")
        time.sleep(2)

    def getProcessedTransactionsNum(self):
        return 1000

    def printResult(self):
        durSec = (self.finish - self.start).seconds
        num = self.getProcessedTransactionsNum()
        print("Performance num: %d dur: %d speed: %d (T/s)" % (num, durSec, float(num) / durSec))
        
    def run(self):
        self.startZooKeepers()
        self.startTPs()
        self.startTCs()
        self.runCC()
        self.waitForReady()
        self.start = datetime.now()
        self.triggerStart()
        self.waitDesiredDuration()
        self.triggerFinish()
        self.waitForDone()
        self.finish = datetime.now()
        self.runTV()
        self.printResult()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate jopama docker cluster configuration')
    parser.add_argument('-ips', type=str, nargs='+', required=True, dest='ips')
    parser.add_argument('-numClusters', type=int, required=True, dest='numClusters')
    parser.add_argument('-clusterSize', type=int, required=True, dest='clusterSize')
    parser.add_argument('-numTP', type=int, required=True, dest='numTP', help='Num transaction processors per cluster')
    parser.add_argument('-numTC', type=int, required=True, dest='numTC', help='Num transaction creators per cluster')
    parser.add_argument('-firstComp', type=int, required=True, dest='firstComp', help="First component")
    parser.add_argument('-numComp', type=int, required=True, dest='numComp', help="Number of components")
    parser.add_argument('-compsInTra', type=int, required=True, dest='compsInTra', help="Components in transaction")
    parser.add_argument('-outForTC', type=int, required=True, dest='outcre', help="Outstanding transactions for creator")
    parser.add_argument('-outForTP', type=int, required=True, dest='outForTP', help="Outstanding transactions for processor")
    args = parser.parse_args()
    if (args.numClusters * args.clusterSize) % len(args.ips) != 0:
        print('Arguments, numClusters * clusterSize should be multiplication of num ips, numClusters: %d, clusterSize: %d num ips: %d' % (args.numClusters, args.clusterSize, len(args.ips)))
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
    test = TestRunner(args, gen.dist)
    test.run()
    sys.exit(0)
