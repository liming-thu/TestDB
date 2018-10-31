import LIMITlib as lmt
import time
import numpy as np
def createStratifiedSample():
    t1=time.time()
    lmt.gridSampleTopCells(870,'top2tweets','grid5')
    t2=time.time()
    print "Total time:",t2-t1

def offlineSample(tab,wlist):
    kwList=wlist##freq: 50k,500k,1M,2M
    rndSampleList=[]
    stratSampleList=[]
    origTab=tab
    for kw in kwList:
        ##A. Original query, get the number of all records that contain the keyword, and time
        freq=len(lmt.GetCoordinateUber(origTab,kw,-1))
        print tab,kw,freq,'null','0','1'

        ##B. Online sampling (LIMIT K), get the number of records of quality=0.85, and time
        k=lmt.kOfHybridQueriesUber(kw,0.85,origTab)
        print tab,kw,k,'null','0','0.85'

        ##C. Online sampling + Offline sampling
        for smp in rndSampleList+stratSampleList:
            k0=len(lmt.GetCoordinateUber(smp, kw, -1))## #records in offline sample
            k1=lmt.kOfHybridQueriesUber(kw,0.85,origTab,smp)
            print tab,kw,k1,smp,k0,'0.85'

def getExeTime(dataset,keyword,k1,smpTab,k0):
    if smpTab[-1]=='3' or smpTab[-1]=='5':
        return
    limitSQL="select * from "+dataset+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k1
    sampleSQL="select * from "+smpTab+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k0
    dummySQL="select count(*) from (select x,y from dummy_table) a"
    dummySQL2="select count(*) from coord_tweets where to_tsvector('english',text)@@to_tsquery('english','veteran')"
    limitT=0.0
    sampleT=0.0
    for i in range(1,6):
        #lmt.restart()
        lmt.cur.execute(dummySQL)
        lmt.cur.execute(dummySQL2)
        ts=time.time()
        print limitSQL
        lmt.cur.execute(limitSQL)
        te=time.time()
        limitT+=te-ts
        print te-ts

        if smpTab!='null':
            ts=time.time()
            lmt.cur.execute(sampleSQL)
            te=time.time()
            sampleT+=te-ts
    return limitT/5.0,sampleT/5.0

def ResponseTime():
    f=open('k.txt')
    lines=f.readlines()
    for line in lines:
        strs=line.split()
        dataset=strs[0]
        keyword=strs[1]
        k1=strs[2]
        smpTab=strs[3]
        k0=strs[4]
        quality=strs[5]
        print getExeTime(dataset,keyword,k1,smpTab,k0)
        #CountBlocks(dataset,keyword,k1,smpTab,k0)
def CountBlocks(dataset,keyword,k1,smpTab,k0):
    explainSQL="explain(analyze,buffers) "+"select * from "+dataset+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k1
    explainSQL2="explain(analyze,buffers) "+"select * from "+smpTab+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k0
    lmt.cur.execute(explainSQL)
    lines=lmt.cur.fetchall()
    blocks2=""
    blocks1=lines[5]
    if smpTab!='null':
        lmt.cur.execute(explainSQL2)
        lines=lmt.cur.fetchall()
        blocks2=lines[5]
    print blocks1,blocks2
def UberCurves():
    cateList=["B02512","B02617","B02682","B02764"]
    for cate in cateList:
        sql="select y,x from uber where base='"+cate+"'"
        lmt.cur.execute(sql)
        coord=np.array(lmt.cur.fetchall())
        originLen=lmt.imageLen(coord)
        for i in range(5,96,5):
            k=int(float(i)/100.0*len(coord))
            aprxLen=lmt.imageLen(coord[:k])
            print cate,i,float(aprxLen)/originLen
        
#timeEval()
#offlineSample('uber',["B02512","B02617","B02682","B02764"])
#offlineSample('coord_tweets_sorted',["soccer","columbia","angel","veteran"])
#offlineSample('coord_tweets_combined',["soccer","columbia","angel","veteran"])
#ResponseTime()
UberCurves()
