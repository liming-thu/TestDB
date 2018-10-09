import psycopg2
import time
import math
import numpy as np
import demjson

res_x = 1920 / 4
res_y = 1080 / 4

y0 = 15.0
y1 = 70.0

x0 = -170.0
x1 = -60.0

yStep=(y1-y0)/res_y
xStep=(x1-x0)/res_x

conStr = "dbname='postgres' user='postgres' host='192.168.137.1' password='liming' "
conn = psycopg2.connect(conStr)
cur = conn.cursor()


# Return the coordinate of keyword from table tb, if limit is -1, then return all the records, order by is the id of the table.
def GetCoordinate(tb, keyword, limit=-1, orderby=False):
    conn = psycopg2.connect(conStr)
    cur = conn.cursor()
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

#fine the trend of k when scaling ataset
def ScaleDataSize():
    for w in ['soccer', 'beach', 'love']:
        coord = GetCoordinate('coordtweets', w, -1)
        print w, len(coord)
        for s in range(10, 101, 10):
            size = s * len(coord) / 100
            scoord = coord[:size]
            r = findkofQ(scoord, 0.85)
            print size, r * size

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

#k: the threshold of #records for each cell
#refTab: the table created by using LIMIT k of original datatable without contains keyword.
def gridSampleTopCells(k,refTab,smpTab):
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
            sql="insert into "+smpTab+" select * from tweets where "+box+"@>coordinate offset "+str(cnt)+" limit "+str(tmpk)##str(tmpoffset)
            cur.execute(sql)
            t2=time.time()
            print res_x,x,res_y,y,cnt,tmpk
            totaltime+=t2-t1
    cur.execute('commit')
    print "Grid Sample: k="+str(k)+", net time:"+str(totaltime)