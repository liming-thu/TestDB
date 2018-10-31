import LIMITlib as lmt
import time

def createStratifiedSample():
    t1=time.time()
    lmt.gridSampleTopCells(870,'top2tweets','grid5')
    t2=time.time()
    print "Total time:",t2-t1

def offlineSample(tab,wlist):
    kwList=wlist##freq: 50k,500k,1M,2M
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
#
def exeSQLonly(kw,tab,k,dummytab='null'):
    sql="select coordinate from "+tab+" where to_tsvector('english',text)@@to_tsquery('english','"+kw+"') "
    if k>0:
        sql+="limit "+str(k)
    t=0.0
    for i in range(1,11):
        if dummytab is not 'null':
            lmt.cur.execute("select count(*) from "+dummytab)
        ts=time.time()
        lmt.cur.execute(sql)
        te=time.time()
        t+=te-ts
    return t/10.0

def timeEval():
   #RANDOM DATASET
    print "RANDOM DATASET"
    #soccer
    print 'soccer random', exeSQLonly('soccer','tweets',54055,'coordtweets')
    print 'soccer random', exeSQLonly('soccer','tweets',32939,'coordtweets')
    #random
    print 'soccer rnd1',exeSQLonly('soccer','rndsample1',-1,'coordtweets'),exeSQLonly('soccer','tweets',32939,'coordtweets')
    print 'soccer rnd3',exeSQLonly('soccer','rndsample3',-1,'coordtweets'),exeSQLonly('soccer','tweets',31250,'coordtweets')
    print 'soccer rnd5',exeSQLonly('soccer','rndsample5',-1,'coordtweets'),exeSQLonly('soccer','tweets',31250,'coordtweets')
#stratified
    print 'soccer grid1',exeSQLonly('soccer','grid1',-1,'coordtweets'),exeSQLonly('soccer','tweets',30405,'coordtweets')
    print 'soccer grid3',exeSQLonly('soccer','grid3',-1,'coordtweets'),exeSQLonly('soccer','tweets',23649,'coordtweets')
    print 'soccer grid5',exeSQLonly('soccer','grid5',-1,'coordtweets'),exeSQLonly('soccer','tweets',16047,'coordtweets')

    #music
    print 'music random', exeSQLonly('music','tweets',502835,'coordtweets')
    print 'music random', exeSQLonly('music','tweets',306415,'coordtweets')
    #random
    print 'music rnd1',exeSQLonly('music','rndsample1',-1,'coordtweets'),exeSQLonly('music','tweets',306415,'coordtweets')
    print 'music rnd3',exeSQLonly('music','rndsample3',-1,'coordtweets'),exeSQLonly('music','tweets',298558,'coordtweets')
    print 'music rnd5',exeSQLonly('music','rndsample5',-1,'coordtweets'),exeSQLonly('music','tweets',298558,'coordtweets')
    #stratified
    print 'music grid1',exeSQLonly('music','grid1',-1,'coordtweets'),exeSQLonly('music','tweets',219990,'coordtweets')
    print 'music grid3',exeSQLonly('music','grid3',-1,'coordtweets'),exeSQLonly('music','tweets',62854,'coordtweets')
    print 'music grid5',exeSQLonly('music','grid5',-1,'coordtweets'),exeSQLonly('music','tweets',7856,'coordtweets')

    #traffic
    print 'traffic random', exeSQLonly('traffic','tweets',960092,'coordtweets')
    print 'traffic random', exeSQLonly('traffic','tweets',510048,'coordtweets')
    #random
    print 'traffic rnd1',exeSQLonly('traffic','rndsample1',-1,'coordtweets'),exeSQLonly('traffic','tweets',510048,'coordtweets')
    print 'traffic rnd3',exeSQLonly('traffic','rndsample3',-1,'coordtweets'),exeSQLonly('traffic','tweets',495047,'coordtweets')
    print 'traffic rnd5',exeSQLonly('traffic','rndsample5',-1,'coordtweets'),exeSQLonly('traffic','tweets',480046,'coordtweets')
    #stratified
    print 'traffic  grid1',exeSQLonly('traffic','grid1',-1,'coordtweets'),exeSQLonly('traffic','tweets',420040,'coordtweets')
    print 'traffic  grid3',exeSQLonly('traffic','grid3',-1,'coordtweets'),exeSQLonly('traffic','tweets',210020,'coordtweets')
    print 'traffic  grid5',exeSQLonly('traffic','grid5',-1,'coordtweets'),exeSQLonly('traffic','tweets',105010,'coordtweets')

    #veteran
    print 'veteran random', exeSQLonly('veteran','tweets',2065024,'coordtweets')
    print 'veteran random', exeSQLonly('veteran','tweets',967980,'coordtweets')
    #random
    print 'veteran rnd1',exeSQLonly('veteran','rndsample1',-1,'coordtweets'),exeSQLonly('veteran','tweets',967980,'coordtweets')
    print 'veteran rnd3',exeSQLonly('veteran','rndsample3',-1,'coordtweets'),exeSQLonly('veteran','tweets',903448,'coordtweets')
    print 'veteran rnd5',exeSQLonly('veteran','rndsample5',-1,'coordtweets'),exeSQLonly('veteran','tweets',903448,'coordtweets')
    #stratified
    print 'veteran  grid1',exeSQLonly('veteran','grid1',-1,'coordtweets'),exeSQLonly('veteran','tweets',774384,'coordtweets')
    print 'veteran  grid3',exeSQLonly('veteran','grid3',-1,'coordtweets'),exeSQLonly('veteran','tweets',169396,'coordtweets')
    print 'veteran  grid5',exeSQLonly('veteran','grid5',-1,'coordtweets'),exeSQLonly('veteran','tweets',2016,'coordtweets')



    #BIASED DATASET
    #soccer
    print 'soccer biased', exeSQLonly('veteran','tweets',54055,'coordtweets')
    print 'soccer biased', exeSQLonly('veteran','tweets',45292,'coordtweets')
    #random
    print "BAISED DATASET"
    print 'soccer rnd1',exeSQLonly('soccer','rndsample1',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',43075,'coordtweets')
    print 'soccer rnd3',exeSQLonly('soccer','rndsample3',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',42230,'coordtweets')
    print 'soccer rnd5',exeSQLonly('soccer','rndsample5',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',37162,'coordtweets')

#stratified
    print 'soccer grid1',exeSQLonly('soccer','grid1',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',43497,'coordtweets')
    print 'soccer grid3',exeSQLonly('soccer','grid3',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',37162,'coordtweets')
    print 'soccer grid5',exeSQLonly('soccer','grid5',-1,'coordtweets'),exeSQLonly('soccer','sortedtweets',33784,'coordtweets')

    #music
    print 'music biased', exeSQLonly('veteran','tweets',502835,'coordtweets')
    print 'music biased', exeSQLonly('veteran','tweets',408553,'coordtweets')
    #random
    print 'music rnd1',exeSQLonly('music','rndsample1',-1,'coordtweets'),exeSQLonly('music','sortedtweets',389893,'coordtweets')
    print 'music rnd3',exeSQLonly('music','rndsample3',-1,'coordtweets'),exeSQLonly('music','sortedtweets',384983,'coordtweets')
    print 'music rnd5',exeSQLonly('music','rndsample5',-1,'coordtweets'),exeSQLonly('music','sortedtweets',382036,'coordtweets')

#stratified
    print 'music grid1',exeSQLonly('music','grid1',-1,'coordtweets'),exeSQLonly('music','sortedtweets',381054,'coordtweets')
    print 'music grid3',exeSQLonly('music','grid3',-1,'coordtweets'),exeSQLonly('music','sortedtweets',322128,'coordtweets')
    print 'music grid5',exeSQLonly('music','grid5',-1,'coordtweets'),exeSQLonly('music','sortedtweets',227847,'coordtweets')

    #traffic
    print 'traffic biased', exeSQLonly('veteran','tweets',960092,'coordtweets')
    print 'traffic biased', exeSQLonly('veteran','tweets',862582,'coordtweets')
    #random
    print 'traffic rnd1',exeSQLonly('traffic','rndsample1',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',838205,'coordtweets')
    print 'traffic rnd3',exeSQLonly('traffic','rndsample3',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',823203,'coordtweets')
    print 'traffic rnd5',exeSQLonly('traffic','rndsample5',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',821328,'coordtweets')
    #stratified
    print 'traffic  grid1',exeSQLonly('traffic','grid1',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',821328,'coordtweets')
    print 'traffic  grid3',exeSQLonly('traffic','grid3',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',705067,'coordtweets')
    print 'traffic  grid5',exeSQLonly('traffic','grid5',-1,'coordtweets'),exeSQLonly('traffic','sortedtweets',615058,'coordtweets')

    #veteran
    print 'veteran biased', exeSQLonly('veteran','tweets',2065024,'coordtweets')
    print 'veteran biased', exeSQLonly('veteran','tweets',1742364,'coordtweets')
    #random
    print 'veteran rnd1',exeSQLonly('veteran','rndsample1',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',1524568,'coordtweets')
    print 'veteran rnd3',exeSQLonly('veteran','rndsample3',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',1427770,'coordtweets')
    print 'veteran rnd5',exeSQLonly('veteran','rndsample5',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',1355172,'coordtweets')
    #stratified
    print 'veteran  grid1',exeSQLonly('veteran','grid1',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',1597167,'coordtweets')
    print 'veteran  grid3',exeSQLonly('veteran','grid3',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',1226108,'coordtweets')
    print 'veteran  grid5',exeSQLonly('veteran','grid5',-1,'coordtweets'),exeSQLonly('veteran','sortedtweets',806650,'coordtweets')

timeEval()
# offlineSample('tweets',["work"])
# offlineSample('sortedtweets',["work"])