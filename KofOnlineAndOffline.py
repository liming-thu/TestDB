import LIMITlib as lmt
import time

def createStratifiedSample():
    t1=time.time()
    lmt.gridSampleTopCells(870,'top2tweets','grid5')
    t2=time.time()
    print "Total time:",t2-t1

def offlineSample(tab):
    kwList=["soccer","music","traffic","veteran"]##freq: 50k,500k,1M,2M
    rndSampleList=["rndSample1","rndSample3","rndSample5"]
    stratSampleList=["grid1","grid3","grid5"]
    origTab=tab
    for kw in kwList:
        ##A. Original query, get the number of all records that contain the keyword, and time
        freq=len(lmt.GetCoordinate(origTab,kw,-1))
        print 'A.',kw,'frequency:',freq

        ##B. Online sampling (LIMIT K), get the number of records of quality=0.85, and time
        k=lmt.kOfHybridQueries(kw,0.85,origTab)
        print 'B.',kw,'k:',k

        ##C. Online sampling + Offline sampling(RS)
        for smp in rndSampleList:
            k0=len(lmt.GetCoordinate(smp, kw, -1))## #records in offline sample
            k1=lmt.kOfHybridQueries(kw,0.85,origTab,smp)
            print "C.Random:",smp,kw,'k0:',k0,'k1:',k1

        ##D. Online sampling + Offline sampling(SS)
        for smp in stratSampleList:
            k0=len(lmt.GetCoordinate(smp, kw, -1))## #records in offline sample
            k1=lmt.kOfHybridQueries(kw,0.85,origTab,smp)
            print "C.Stratified:",smp,kw,'k0:',k0,'k1:',k1
offlineSample('tweets')