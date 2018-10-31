import psycopg2
import time
import math
import numpy as np
import sys
import os
import demjson

res_x = 1920 / 4
res_y = 1080 / 4

#coordinates of New York
y0_ny = 39.65
y1_ny = 42.12

x0_ny = -74.95
x1_ny = -72.0

y0 = 15.0
y1 = 70.0

x0 = -170.0
x1 = -60.0

yStep=(y1-y0)/res_y
xStep=(x1-x0)/res_x


#postgresql connection
conStr = "dbname='limitdb2' user='postgres' host='localhost' password='postgres' "
conn = psycopg2.connect(conStr)
cur = conn.cursor()

#oracle connection
ora_conn=cx_Oracle.connect("system","Oracle123","curium.ics.uci.edu:1521/orcl")
ora_cur=ora_conn.cursor()

def restart(version=9.6):
        if sys.platform == 'darwin':
            os.system('brew services stop postgresql')
            os.system('brew services start postgresql')
        elif sys.platform == 'linux2':
            if version >= 9.5:
                print 'sudo systemctl restart postgresql-' + str(version)
                os.system('sudo systemctl restart postgresql-' + str(version))
            else:
                os.system('sudo systemctl restart postgresql')

        i = 0
        while i <= 10:
            try:
                conn = psycopg2.connect(conStr)
                cur = conn.cursor()
                break
            except psycopg2.DatabaseError:
                print 'wait 1s for db restarting ... ...'
                time.sleep(1)
                i += 1
        if i > 10:
            raise psycopg2.DatabaseError

# Return the coordinate of keyword from table tb, if limit is -1, then return all the records, order by is the id of the table.
def GetCoordinate(tb, keyword, limit=-1, orderby=False):
    conn = psycopg2.connect(conStr)
    cur = conn.cursor()
    sql = "select count(*) from information_schema.columns where table_name='"+tb+"' and column_name='coordinate'"
    cur.execute(sql)
    hasPoint=cur.fetchall()[0][0]
    if int(hasPoint) == 1:
        sql = " select coordinate[0],coordinate[1] from " + tb + " where to_tsvector('english',text)@@to_tsquery('english','" + keyword + "')"
    else:
        sql = "select x,y from " + tb + " where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
    if orderby:
        sql += " order by id"
    if limit >= 0:
        sql += " limit " + str(limit)
    cur.execute(sql)
    return cur.fetchall()
# get coordinates from oracle
def GetCoordinateOra(tb, keyword, limit=-1, orderby=False):
    sql = "select x,y from " + tb + " where contains(text,'"+keyword+"')>0"
    if orderby:
        sql += " order by id"
    if limit >= 0:
        sql += " where rownum>= " + str(limit)
    ora_cur.execute(sql)
    return ora_cur.fetchall()
def GetCoordinateUber(tb, base, limit=-1, orderby=False):
    conn = psycopg2.connect(conStr)
    cur = conn.cursor()
    sql = "select y,x from " + tb + " where base='"+base+"'"
    if limit >= 0:
        sql = " select coordinate[0],coordinate[1] from " + tb + " where to_tsvector('english',text)@@to_tsquery('english','" + keyword + "')"
    if orderby:
        sql += " order by id"
    if limit >= 0:
        sql += " limit " + str(limit)
    cur.execute(sql)
    return cur.fetchall()

# Return the keywords in table tb, the lower and upper are the frequency bounds, k is the limit number of returned keywords.
def GetKeywords(tb, lower, upper, k):
    conn = psycopg2.connect(conStr)
    cur = conn.cursor()
    sql = "select vector,count from " + tb + " where count>=" + str(lower) + " and count<" + str(
        upper) + "order by count limit " + str(
        k)  # +" and vector not in (select distinct keyword from keyword_k_q) order by count"
    cur.execute(sql)
    return cur.fetchall()
# Map the coodrinates into cells, the type of 'ar' is the numpy array, r is the coordinate range of the map. the returned value H is the matrix of cells,
# each value is the number of records in the cell.
def hashByNumpy(ar, r=((-170, -60), (15, 70))):
    H, x, y = np.histogram2d(ar[:, 0], ar[:, 1], bins=(res_x, res_y), range=r)
    return H
# the number of non-zero cells
def imageLen(array):
    return np.count_nonzero(hashByNumpy(array))

# return the mse of two matrix
def myMSE(m1,m2, binary=True):#m1, m2 are the matrixs of the ground-truth map and approximate map
    if binary:
        m1=np.where(m1>0,1,0) #convert each element to 0 or 1
        m2=np.where(m2>0,1,0) #convert each element to 0 or 1
    err=np.sum((m1-m2)**2)
    err/=float(len(m1)*len(m1[0]))
    return err
#Use binary search to find the k that have quality Q in coordinate ar.
def findkofQ(ar, Q):
    perfectLen = imageLen(np.array(ar))
    i = 0.0
    l = 0.0
    h = 100.0
    similarity = 0.0
    iterTimes = 0
    while (similarity < 0.85 or similarity > 0.86) and iterTimes < 10:
        if similarity < 0.85:
            l = i
            i = (h + i) / 2
        else:
            h = i
            i = (i + l) / 2
        k = int(i * len(ar) / 100)
        sampleLen = imageLen(np.array(ar[:k]))
        similarity = float(sampleLen) / perfectLen
        iterTimes += 1
    return i

#Find the k of hybrid queries, w:keyword, q:quality, tb: original data table, hybridtab: offline sample table
def kOfHybridQueries(w, q, tb,hybridtab='null'):
    coord = GetCoordinate(tb, w, -1)
    if len(coord) < 5000:
        return 0
    offlineHs = np.zeros(shape=(480, 270), dtype=int)
    if hybridtab is not 'null':
        offlinecoord = GetCoordinate(hybridtab, w, -1)
        offlineHs = hashByNumpy(np.array(offlinecoord))
    ar = np.array(coord)
    H = hashByNumpy(ar)#matrix of from the original data table
    perfectLen = np.count_nonzero(H)
    i = 0.0
    l = 0.0
    h = 100.0
    similarity = 0.0
    iterTimes = 0
    #binary search of k for quality q, max iteration times is 20
    while (similarity < q or similarity > q * 1.01) and iterTimes < 20:
        if similarity < q:
            l = i
            i = (h + i) / 2
        else:
            h = i
            i = (i + l) / 2
        k = int(i * len(ar) / 100)
        Hs = hashByNumpy(ar[:k])
        if hybridtab is not 'null':#combine the online subset with the offline subset
            Hs += offlineHs
        sampleLen = np.count_nonzero(Hs)
        similarity = float(sampleLen) / perfectLen
        iterTimes += 1
    return k
def kOfHybridQueriesUber(w, q, tb,hybridtab='null'):
    coord = GetCoordinateUber(tb, w, -1)
    if len(coord) < 5000:
        return 0
    offlineHs = np.zeros(shape=(480, 270), dtype=int)
    if hybridtab is not 'null':
        offlinecoord = GetCoordinateUber(hybridtab, w, -1)
        offlineHs = hashByNumpy(np.array(offlinecoord))
    ar = np.array(coord)
    H = hashByNumpy(ar)#matrix of from the original data table
    perfectLen = np.count_nonzero(H)
    i = 0.0
    l = 0.0
    h = 100.0
    similarity = 0.0
    iterTimes = 0
    #binary search of k for quality q, max iteration times is 20
    while (similarity < q or similarity > q * 1.01) and iterTimes < 20:
        if similarity < q:
            l = i
            i = (h + i) / 2
        else:
            h = i
            i = (i + l) / 2
        k = int(i * len(ar) / 100)
        Hs = hashByNumpy(ar[:k])
        if hybridtab is not 'null':#combine the online subset with the offline subset
            Hs += offlineHs
        sampleLen = np.count_nonzero(Hs)
        similarity = float(sampleLen) / perfectLen
        iterTimes += 1
    return k
# find k of hybird queries in oracle, the only difference is call of GetCoordinate
def kOfHybridQueriesOra(w, q, tb,hybridtab='null'):
    coord = GetCoordinateOra(tb, w, -1)
    if len(coord) < 5000:
        return 0
    offlineHs = np.zeros(shape=(480, 270), dtype=int)
    if hybridtab is not 'null':
        offlinecoord = GetCoordinateOra(hybridtab, w, -1)
        offlineHs = hashByNumpy(np.array(offlinecoord))
    ar = np.array(coord)
    H = hashByNumpy(ar)#matrix of from the original data table
    perfectLen = np.count_nonzero(H)
    i = 0.0
    l = 0.0
    h = 100.0
    similarity = 0.0
    iterTimes = 0
    #binary search of k for quality q, max iteration times is 20
    while (similarity < q or similarity > q * 1.01) and iterTimes < 20:
        if similarity < q:
            l = i
            i = (h + i) / 2
        else:
            h = i
            i = (i + l) / 2
        k = int(i * len(ar) / 100)
        Hs = hashByNumpy(ar[:k])
        if hybridtab is not 'null':#combine the online subset with the offline subset
            Hs += offlineHs
        sampleLen = np.count_nonzero(Hs)
        similarity = float(sampleLen) / perfectLen
        iterTimes += 1
    return k
#fine the trend of k when scaling ataset
def ScaleDataSize(kwList,tab):
    for w in kwList:
        coord = GetCoordinate(tab, w, -1)
        for s in range(10, 101, 10):
            size = s * len(coord) / 100
            scoord = coord[:size]
            r = findkofQ(scoord, 0.85)
            print w, 'dataset size:',size,'85% quality:',r * size/100

#Load state polygons to db from file
def loadStatePolygon():
    poly = demjson.decode_file("state.json")
    for state in poly['features']:
        name = state['properties']['name']
        polys = state['geometry']['coordinates']
        for p in polys:
            coords = "'" + str(p).replace('[', '(').replace(']', ')')[1:-1] + "'"
            sql = "insert into statepolygon values('" + name + "'," + coords + ")"
            cur.execute(sql)
            print name
    cur.execute('commit')

#update the column of state in coordtweets
def updateStateField():
    cur.execute("select id from coordtweets")
    ids = cur.fetchall()
    i = 0
    for id in ids:
        name = "NULL"
        cur.execute("select state from statepolygon where poly@>(select coordinate from coordtweets where id=" + str(
            id[0]) + ") limit 1")
        res = cur.fetchall()
        if len(res) > 0:
            name = res[0][0]
        cur.execute("update coordtweets set state='" + name + "' where id=" + str(id[0]))
        i += 1
        print i, id
    cur.execute('commit')

#produce count map of subset LIMIT k
def countMap(w, k=4000000):
    sql = "select state, count(*) from (select state,id from coordtweets where to_tsvector('english',text)@@to_tsquery('english','" + w + "') limit " + str(
        k) + ") t group by t.state"
    cur.execute(sql)
    return cur.fetchall()

#use the distributed precision to compute the count map quality, s and e are the start and end frequency
def countMapQuality(s, e):
    keywords = GetKeywords('vectorcount', s, e, 1000)
    for w in keywords:
        gt = dict((x, y) for x, y in countMap(w[0]))
        for i in gt.keys():
            gt[i] = float(gt[i]) / w[1]
        for r in range(1, 101, 1):
            k = r * w[1] / 100
            sub = dict((x, y) for x, y in countMap(w[0], k))
            for i in sub.keys():
                sub[i] = float(sub[i]) / k
            e = 0.0
            for i in gt.keys():
                if sub.has_key(i):
                    e += math.pow((gt[i] - sub[i]), 2)
                else:
                    e += math.pow(gt[i], 2)
            print w[0], r / 1000, k, math.sqrt(e)


def getError(gt, freq, sub, k):
    e = 0.0
    for i in gt.keys():
        if sub.has_key(i):
            e += math.pow(float(gt[i]) / freq - float(sub[i]) / k, 2)
        else:
            e += math.pow(float(gt[i]) / freq, 2)
    return math.sqrt(e)


def countMapQualityMem(s, e):
    keywords = GetKeywords('vectorcount', s, e, 1000)
    for w in keywords:
        cur.execute(
            "select state from coordtweets where to_tsvector('english',text)@@to_tsquery('english','" + w[0] + "')")
        res = cur.fetchall()
        freq = len(res)
        gt = {}
        for i in res:
            if gt.has_key(i[0]):
                gt[i[0]] += 1
            else:
                gt[i[0]] = 1
    for r in range(1, 201, 1):
        k = r * w[1] / 1000
        sub = {}
        for i in range(0, k):
            if sub.has_key(res[i][0]):
                sub[res[i][0]] += 1
            else:
                sub[res[i][0]] = 1
        print w[0], float(k) / w[1], getError(gt, freq, sub, k)

#compare k in online, online+offline, s,e are the start and end frequencies, tb is the original data table
def kComparison(s, e, tb):
    keywords = GetKeywords('vectorcount', s, e, 100)
    for w in keywords:
        online = kOfHybridQueries(w[0], 0.85, tb)#online
        offset0 = (kOfHybridQueries(w[0], 0.85, tb, 'gridsample0'))#online+stratified sample
        offset50 = (kOfHybridQueries(w[0], 0.85, tb, 'gridsample50'))#onlien+stratified sample+ sample from tail
        offsetalpha = (kOfHybridQueries(w[0], 0.85, tb, 'gridsample'))#onlien+stratified sample+ sample from tail+reducing #records in cells of LIMIT k
        if online > 0:
            print w[0], w[1], online, offset0, offset50, offsetalpha
# find the k of each cell that how many records need to be scanned to find the keyword
def FindFirstIndexofKeyword(keyword):
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sql="select text from coordtweets where "+box+"@>coordinate"
            cur.execute(sql)
            texts=cur.fetchall()
            i=0
            found=False
            for text in texts:
                i+=1
                sql="select to_tsvector('english','"+text[0].replace("'"," ")+"')@@to_tsquery('english','"+keyword+"')"
                cur.execute(sql)
                result=cur.fetchall()
                if result[0][0]:
                    found=True
                    break
            if found:
                cur.execute("insert into firstindex values('"+keyword+"',"+str(x)+","+str(y)+","+str(i)+","+str(len(texts))+")")
            else:
                cur.execute("insert into firstindex values('"+keyword+"',"+str(x)+","+str(y)+","+str(0)+","+str(len(texts))+")")
            cur.execute("commit")
#find the max density of the map
def maxDensity(tb):
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sqlcnt="select count(*) from "+tb+" where "+box+"@>coordinate"
            cur.execute(sqlcnt)
            cnt=cur.fetchall()
            if cnt[0][0]>dmax:
                dmax=cnt[0][0]

#alpha=0: use pure stratified sampling
#alpha=x: the #records in each cell is proportional to its density, the cells that density>(1/alpha)* max_density have no records.
def stratifiedSampling(k,alpha=0):
    i=0
    j=0
    dmax=maxDensity('coordtweets')#the max density of coordtweets is 399,000.
    for x in range(0,res_x):
        for y in range(0,res_y):
            tmpoffset=0
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sqlcnt="select count(*) from coordtweets where "+box+"@>coordinate"
            cur.execute(sqlcnt)
            cnt=cur.fetchall()
            if cnt[0][0]==0:
                continue
            tmpk=int(k*float(max(0,dmax-alpha*cnt[0][0]))/dmax)
            if cnt[0][0]>=tmpk:
                tmpoffset=cnt[0][0]-tmpk
            else:
                tmpoffset=0
            sql="insert into gridsample select * from coordtweets where "+box+"@>coordinate offset "+str(tmpoffset)+" limit "+str(tmpk)
            cur.execute(sql)
            print res_x,x,res_y,y,cnt[0][0],tmpoffset,tmpk
    cur.execute('commit')
    print "Grid Sample: k="+str(k)
# Get curves of keyword w in table tab, start k=10%, end k=90%
def Curves(w,tab):
    coord=GetCoordinate(tab,w)
    print w,len(coord)
    perfectImageLen=imageLen(np.array(coord))
    for r in range(10,100,10):
        subLen=int(float(r)*len(coord)/100.0)
        aprxImageLen=imageLen(np.array(coord[:subLen]))
        print r,float(aprxImageLen)/perfectImageLen

#k: the threshold of #records for each cell
#refTab: the table created by using LIMIT k of original datatable without contains keyword.
def gridSampleTopCells(k,refTab,smpTab,srcTab):
    cur.execute("create table if not exists "+smpTab+" as select * from tweets where 1=2")
    cur.execute("commit")
    totaltime=0
    for x in range(0,res_x):
        for y in range(0,res_y):
            tmpoffset=0
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            #remove top n cells
            sql="select count(*) from "+refTab+" where "+box+"@>coordinate"
            cur.execute(sql)
            cnt=cur.fetchall()[0][0]
            if cnt>=k:
                continue
            else:
                tmpk=k-cnt
            # sqlcnt="select count(*) from tweets where "+box+"@>coordinate"
            # cur.execute(sqlcnt)
            # cnt=cur.fetchall()
            # if cnt[0][0]>=tmpk:
            #     tmpoffset=cnt[0][0]-tmpk
            # else:
            #     tmpoffset=0
            # if cnt[0][0]>0:
            t1=time.time()
            sql="insert into "+smpTab+" select * from "+srcTab+" where "+box+"@>coordinate offset "+str(cnt)+" limit "+str(tmpk)##str(tmpoffset)
            cur.execute(sql)
            t2=time.time()
            print res_x,x,res_y,y,cnt,tmpk
            totaltime+=t2-t1
    cur.execute('commit')
    print "Grid Sample: k="+str(k)+", net time:"+str(totaltime)
    
# using the random function to get a random sample for each cell.
def gridSampleRandomFunction():
    cur.execute("create table if not exists vas_ss3 as select * from tweets where 1=2")
    cur.execute("commit")
    totaltime=0
    for x in range(0,res_x):
        for y in range(0,res_y):
            tmpoffset=0
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            #remove top n cells
            sql="select count(*) from ss3 where "+box+"@>coordinate"
            cur.execute(sql)
            cnt=cur.fetchall()[0][0]
            if cnt==0:
                continue
            print x,y
            sql="select count(*) from rnd5 where "+box+"@>coordinate"
            cur.execute(sql)
            cnt2=cur.fetchall()[0][0]
            r=float(cnt)/float(cnt2)
            sql="insert into vas_ss3 select * from rnd5 where "+box+"@>coordinate and random()<="+str(r)
            cur.execute(sql)
    cur.execute('commit')
    print "Grid Sample: k="+str(k)+", net time:"+str(totaltime)
#get k of original, offline sample. tab is the original data table, ss is the sample lsit, wlist is the keyword list, quality is the specified quality
def KofQueries(tab,ss,wlist,quality):
    kwList=wlist##freq: 50k,500k,1M,2M
    stratSampleList=[ss]
    origTab=tab
    for kw in kwList:
        ##A. Original query, get the number of all records that contain the keyword, and time
        freq=len(lmt.GetCoordinate(origTab,kw,-1))
        print tab,kw,freq,'null','0','1'

        ##B. Online sampling (LIMIT K), get the number of records of quality=quality, and time
        for q in quality:
            k=lmt.kOfHybridQueries(kw,q,origTab)
            print tab,kw,k,'null','0',q

            ##C. Online sampling + Offline sampling
            for smp in stratSampleList:
                k0=len(lmt.GetCoordinate(smp, kw, -1))## #records in offline sample
                k1=lmt.kOfHybridQueries(kw,q,origTab,smp)
                print tab,kw,k1,smp,k0,q
                
# get execution time of keyword on table. dataset is the original data table, k1 is the k of original data table, smpTab is the sample table, k0 is the k of sample table.
def getExeTime(dataset,keyword,k1,smpTab,k0):
    limitSQL="select * from "+dataset+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k1
    sampleSQL="select * from "+smpTab+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+k0
    dummySQL="select count(*) from (select x,y from dummy_table) a"
    dummySQL2="select count(*) from coord_tweets where to_tsvector('english',text)@@to_tsquery('english','veteran')"
    limitT=0.0
    sampleT=0.0
    for i in range(1,3):
        #lmt.restart()
        lmt.cur.execute(dummySQL)
        lmt.cur.execute(dummySQL2)
        ts=time.time()
        lmt.cur.execute(limitSQL)
        te=time.time()
        limitT+=te-ts
        if smpTab!='null':
            ts=time.time()
            lmt.cur.execute(sampleSQL)
            te=time.time()
            sampleT+=te-ts
    return limitT/2.0,sampleT/2.0
# get the accessed blocks of a query in postgresql.
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
    print "Grid Sample: k="+str(k)+", net time:"+str(totaltime)
