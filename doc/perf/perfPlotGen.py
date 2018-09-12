import sys
import matplotlib.pyplot as plt
import numpy as np

def readData(dataFile):
    data=[]
    with open(dataFile) as f:
        for line in f:
            items = line.split(' ')
            assert len(items) == 4
            tup = (int(items[0]), int(items[1]), int(items[2]), float(items[3]))
            data.append(tup)
    return data

def draw(nml, results):
    for key in results.keys():
        revKey = tuple(reversed(key))
        sr = results[key]
        f, ax = plt.subplots()
        label = 'TS_%02d_NC_%06d' % revKey
        ax.plot(nml, sr, marker='o')
        ax.set_ylim(ymin=0)
        ax.set_xticks(nml)
        ax.set_xlabel('Num Machines')
        ax.set_ylabel('Transactions Per Sec')
        ax.set_title('Performance, transactionSize: %d, numComps: %d' % revKey)
        f.savefig('perf_%s.png' % label)

def drawOld(nml, results):
    f, axarr = plt.subplots(6)
    for key in sorted(results.keys()):
        pos = sorted(results.keys()).index(key)
        assert pos >= 0 and pos < len(results)
        pos = len(results) - 1 - pos
        sr = results[key]
        name = 'nc_%d_cpt_%d' % key
        ax = axarr[pos]
        ax.plot(nml, sr, label=name)
        ax.legend(loc='best')
    plt.xlabel('Machines')
    plt.ylabel('Trans/sec')
    plt.title('Performance')
    plt.savefig('Performance.png')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: dataFile')
        sys.exit(1)
    data = readData(sys.argv[1])
    for tup in data:
        print('%d %d %d %6.2f' % tup)
    nml = sorted(list(set([tup[0] for tup in data])))
    ncl = sorted(list(set([tup[1] for tup in data])))
    cptl = sorted(list(set([tup[2] for tup in data])))
    plotData = {}
    for cpt in cptl:
        for nc in ncl:
            subRes = {}
            key = (nc, cpt)
            plotData[key] = []
            for nm in nml:
                res = [tup[3] for tup in data if tup[0] == nm and tup[1] == nc and tup[2] == cpt]
                assert len(res) == 1
                plotData[key].append(res[0])
    for kt in plotData.keys():
        print('%s -> %s' % (str(kt), str(plotData[kt])))
    draw(nml, plotData)
