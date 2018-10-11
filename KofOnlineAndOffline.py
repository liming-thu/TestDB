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
def exeSQLonly(kw,tab,k,dummytab='null'):
    sql="select coordinate from "+tab+" where to_tsvector('english',text)@@to_tsquery('english','"+kw+"') "
    if k>0:
        sql+="limit "+str(k)
    t=0.0
    for i in range(1,6):
        if dummytab is not 'null':
            lmt.cur.execute("select count(*) from "+dummytab)
        ts=time.time()
        lmt.cur.execute(sql)
        te=time.time()
        t=te-ts
    return t/10.0

def timeEval():
    #RANDOM DATASET
    print "RANDOM DATASET"
    #soccer
    #random
    print 'soccer rnd1',exeSQLonly('soccer','rndsample1',-1,'sortedtweets')
    print 'soccer 32939',exeSQLonly('soccer','tweets',32939)
    print 'soccer rnd3',exeSQLonly('soccer','rndsample3',-1)
    print 'soccer 31250',exeSQLonly('soccer','tweets',31250,'sortedtweets')
    print 'soccer rnd5',exeSQLonly('soccer','rndsample5',-1)
    #stratified
    print 'soccer grid1',exeSQLonly('soccer','grid1',-1)
    print 'soccer 30405',exeSQLonly('soccer','tweets',30405,'sortedtweets')
    print 'soccer grid3',exeSQLonly('soccer','grid3',-1)
    print 'soccer 23649',exeSQLonly('soccer','tweets',23649,'sortedtweets')
    print 'soccer grid5',exeSQLonly('soccer','grid5',-1)
    print 'soccer 16047',exeSQLonly('soccer','tweets',16047,'sortedtweets')

    #music
    #random
    print 'music rnd1',exeSQLonly('music','rndsample1',-1)
    print 'music 306415',exeSQLonly('music','tweets',306415,'sortedtweets')
    print 'music rnd3',exeSQLonly('music','rndsample3',-1)
    print 'music 298558',exeSQLonly('music','tweets',298558,'sortedtweets')
    print 'music rnd5',exeSQLonly('music','rndsample5',-1)
    #stratified
    print 'music grid1',exeSQLonly('music','grid1',-1)
    print 'music 219990',exeSQLonly('music','tweets',219990,'sortedtweets')
    print 'music grid3',exeSQLonly('music','grid3',-1)
    print 'music 62854',exeSQLonly('music','tweets',62854,'sortedtweets')
    print 'music grid5',exeSQLonly('music','grid5',-1)
    print 'music 7856',exeSQLonly('music','tweets',7856,'sortedtweets')

    #traffic
    #random
    print 'traffic rnd1',exeSQLonly('traffic','rndsample1',-1)
    print 'traffic 510048',exeSQLonly('traffic','tweets',510048,'sortedtweets')
    print 'traffic rnd3',exeSQLonly('traffic','rndsample3',-1)
    print 'traffic 495047',exeSQLonly('traffic','tweets',495047,'sortedtweets')
    print 'traffic rnd5',exeSQLonly('traffic','rndsample5',-1)
    print 'traffic 480046',exeSQLonly('traffic','tweets',480046,'sortedtweets')
    #stratified
    print 'traffic  grid1',exeSQLonly('traffic','grid1',-1)
    print 'traffic  420040',exeSQLonly('traffic','tweets',420040,'sortedtweets')
    print 'traffic  grid3',exeSQLonly('traffic','grid3',-1)
    print 'traffic  210020',exeSQLonly('traffic','tweets',210020,'sortedtweets')
    print 'traffic  grid5',exeSQLonly('traffic','grid5',-1)
    print 'traffic 105010',exeSQLonly('traffic','tweets',105010,'sortedtweets')

    #veteran
    #random
    print 'veteran rnd1',exeSQLonly('veteran','rndsample1',-1)
    print 'veteran 967980',exeSQLonly('veteran','tweets',967980,'sortedtweets')
    print 'veteran rnd3',exeSQLonly('veteran','rndsample3',-1)
    print 'veteran 903448',exeSQLonly('veteran','tweets',903448,'sortedtweets')
    print 'veteran rnd5',exeSQLonly('veteran','rndsample5',-1)
    print 'veteran 903448',exeSQLonly('veteran','tweets',903448,'sortedtweets')
    #stratified
    print 'veteran  grid1',exeSQLonly('veteran','grid1',-1)
    print 'veteran  420040',exeSQLonly('veteran','tweets',774380,'sortedtweets')
    print 'veteran  grid3',exeSQLonly('veteran','grid3',-1)
    print 'veteran  210020',exeSQLonly('veteran','tweets',169396,'sortedtweets')
    print 'veteran  grid5',exeSQLonly('veteran','grid5',-1)
    print 'veteran 105010',exeSQLonly('veteran','tweets',2016,'sortedtweets')



    #BIASED DATASET
    #soccer
    #random
    print "BAISED DATASET"
    print 'soccer rnd1',exeSQLonly('soccer','rndsample1',-1,'tweets')
    print 'soccer 43075',exeSQLonly('soccer','sortedtweets',43075)
    print 'soccer rnd3',exeSQLonly('soccer','rndsample3',-1)
    print 'soccer 42230',exeSQLonly('soccer','sortedtweets',42230,'tweets')
    print 'soccer rnd5',exeSQLonly('soccer','rndsample5',-1)
    print 'soccer 37162',exeSQLonly('soccer','sortedtweets',37162,'tweets')

#stratified
    print 'soccer grid1',exeSQLonly('soccer','grid1',-1)
    print 'soccer 43497',exeSQLonly('soccer','sortedtweets',43497,'tweets')
    print 'soccer grid3',exeSQLonly('soccer','grid3',-1)
    print 'soccer 37162',exeSQLonly('soccer','sortedtweets',37162,'tweets')
    print 'soccer grid5',exeSQLonly('soccer','grid5',-1)
    print 'soccer 33784',exeSQLonly('soccer','sortedtweets',33784,'tweets')

    #music
    #random
    print 'music rnd1',exeSQLonly('music','rndsample1',-1)
    print 'music 389893',exeSQLonly('music','sortedtweets',389893,'tweets')
    print 'music rnd3',exeSQLonly('music','rndsample3',-1)
    print 'music 384983',exeSQLonly('music','sortedtweets',384983,'tweets')
    print 'music rnd5',exeSQLonly('music','rndsample5',-1)
    print 'music 382036',exeSQLonly('music','sortedtweets',382036,'tweets')

#stratified
    print 'music grid1',exeSQLonly('music','grid1',-1)
    print 'music 381054',exeSQLonly('music','sortedtweets',381054,'tweets')
    print 'music grid3',exeSQLonly('music','grid3',-1)
    print 'music 322128',exeSQLonly('music','sortedtweets',322128,'tweets')
    print 'music grid5',exeSQLonly('music','grid5',-1)
    print 'music 227847',exeSQLonly('music','sortedtweets',227847,'tweets')

    #traffic
    #random
    print 'traffic rnd1',exeSQLonly('traffic','rndsample1',-1)
    print 'traffic 838205',exeSQLonly('traffic','sortedtweets',838205,'tweets')
    print 'traffic rnd3',exeSQLonly('traffic','rndsample3',-1)
    print 'traffic 823203',exeSQLonly('traffic','sortedtweets',823203,'tweets')
    print 'traffic rnd5',exeSQLonly('traffic','rndsample5',-1)
    print 'traffic 821328',exeSQLonly('traffic','sortedtweets',821328,'tweets')
    #stratified
    print 'traffic  grid1',exeSQLonly('traffic','grid1',-1)
    print 'traffic  821328',exeSQLonly('traffic','sortedtweets',821328,'tweets')
    print 'traffic  grid3',exeSQLonly('traffic','grid3',-1)
    print 'traffic  705067',exeSQLonly('traffic','sortedtweets',705067,'tweets')
    print 'traffic  grid5',exeSQLonly('traffic','grid5',-1)
    print 'traffic  615058',exeSQLonly('traffic','sortedtweets',615058,'tweets')

    #veteran
    #random
    print 'veteran rnd1',exeSQLonly('veteran','rndsample1',-1)
    print 'veteran 1524568',exeSQLonly('veteran','sortedtweets',1524568,'tweets')
    print 'veteran rnd3',exeSQLonly('veteran','rndsample3',-1)
    print 'veteran 1427770',exeSQLonly('veteran','sortedtweets',1427770,'tweets')
    print 'veteran rnd5',exeSQLonly('veteran','rndsample5',-1)
    print 'veteran 1355172',exeSQLonly('veteran','sortedtweets',1355172,'tweets')
    #stratified
    print 'veteran  grid1',exeSQLonly('veteran','grid1',-1)
    print 'veteran  1597167',exeSQLonly('veteran','sortedtweets',1597167,'tweets')
    print 'veteran  grid3',exeSQLonly('veteran','grid3',-1)
    print 'veteran  1226108',exeSQLonly('veteran','sortedtweets',1226108,'tweets')
    print 'veteran  grid5',exeSQLonly('veteran','grid5',-1)
    print 'veteran  806650',exeSQLonly('veteran','sortedtweets',806650,'tweets')

# timeEval()