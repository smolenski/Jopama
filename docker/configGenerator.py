import argparse
import doctest
import sys

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
        ntpOnIp={}
        ntcOnIp={}
        for ip in args.ips:
            ntpOnIp[ip]=0
            ntcOnIp[ip]=0    
        for cid in range(args.nc):
            gIdAndNumP=[]
            gIdAndNumC=[]
            for rid in range(args.cs):
                gId = cid * args.cs + rid
                ip = args.ips[gId % len(args.ips)]
                gIdAndNumP.append((gId, ntpOnIp[ip]))
                gIdAndNumC.append((gId, ntcOnIp[ip]))
            tpss=[a[0] for a in sorted(gIdAndNumP, key=lambda x: x[1])][:args.ntp]
            tcss=[a[0] for a in sorted(gIdAndNumC, key=lambda x: x[1])][:args.ntc]
            for rid in range(args.cs):
                gId = cid * args.cs + rid
                ip = args.ips[gId % len(args.ips)]
                if gId in tpss:
                    ntpOnIp[ip] = ntpOnIp[ip] + 1
                if gId in tcss:
                    ntcOnIp[ip] = ntcOnIp[ip] + 1
                inst = Instance(gId, cid, rid, ip, gId in tpss, gId in tcss)
                self.dist.append(inst)
        assert max(ntpOnIp.values()) - min(ntpOnIp.values()) <= 1
        assert max(ntcOnIp.values()) - min(ntcOnIp.values()) <= 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate jopama docker cluster configuration')
    parser.add_argument('-ips', type=str, nargs='+', required=True, dest='ips')
    parser.add_argument('-nc', type=int, required=True, dest='nc')
    parser.add_argument('-cs', type=int, required=True, dest='cs')
    parser.add_argument('-ntp', type=int, required=True, dest='ntp', help='Num transaction processors per cluster')
    parser.add_argument('-ntc', type=int, required=True, dest='ntc', help='Num transaction creators per cluster')
    args = parser.parse_args()
    if (args.nc * args.cs) % len(args.ips) != 0:
        print('Arguments, nc * cs should be multiplication of num ips, nc: %d, cs: %d num ips: %d' % (args.nc, args.cs, len(args.ips)))
        sys.exit(1)
    assert args.ntp > 0
    assert args.ntp <= args.cs
    assert args.ntc > 0
    assert args.ntc <= args.cs
    print("args: %s" % args)
    gen = ConfigGenerator(args)
    gen.generate()
    print('DIST BEGIN')
    for inst in gen.dist:
        print('%s' % str(inst))
    print('DIST END')
    sys.exit(0)
