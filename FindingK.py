import psycopg2
import cv2 as cv
import matplotlib.pyplot as plt
import os
import time
import math
import numpy as np
import demjson

res_x=1920/4
res_y=1080/4

y0=15.0
y1=70.0

x0=-170.0
x1=-60.0
conStr="dbname='postgres' user='postgres' host='169.234.49.169' password='liming' "
conn=psycopg2.connect(conStr)
cur=conn.cursor()

def imageHashFromCoordinates(coord):
    pixel_x=(x1-x0)/res_x
    pixel_y=(y1-y0)/res_y
    image={}
    for c in coord:
        x=float(c[0])
        y=float(c[1])
        if y<y0 or y>y1 or x>x1 or x<x0:
            continue
        else:
            index_x=int(math.floor((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            key=str(index_x)+":"+str(index_y)
            if key not in image.keys():
                image[key]=1
    return image
def deltaImageHashFromCoordinates(coord,image):
    pixel_x=(x1-x0)/res_x
    pixel_y=(y1-y0)/res_y
    deltaImage={}
    for c in coord:
        x=float(c[0])
        y=float(c[1])
        if y<y0 or y>y1 or x>x1 or x<x0:
            continue
        else:
            index_x=int(math.floor((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            key=str(index_x)+":"+str(index_y)
            if key not in image.keys():
                deltaImage[key]=1
    return deltaImage
def dhash(image):
    y=len(image)
    x=len(image[0])
    resized=cv.resize(image,(x,y))
    ret=""
    #remove the x and y axils
    for i in range(55,1344):
        for j in range(133,1864):
            if resized[i][j]>230:
                ret+='1'
            else:
                ret+='0'
    return ret
def GetPerceptualHash(file):
    image=cv.imread(file)
    image=cv.cvtColor(image,cv.COLOR_RGB2GRAY)
    return dhash(image)
def GetGrayImage(file):
    image=cv.imread(file)
    image=cv.cvtColor(image,cv.COLOR_RGB2GRAY)
    return image
def GetSimilarity(p1,p2):
    one=0.0
    diff=0.0
    for i in range(0,len(p1)):
        if(p1[i]==p2[i]=='1'):
            one+=1
        else :
            if p1[i]!=p2[i]:
                diff+=1
    similarity=1-diff/(len(p1)-one)
    return similarity
def GetSililarityFromImages(p1,p2):
    one=0.0
    diff=0.0
    a=0
    b=0
    for i in range(55,1344):
        for j in range(133,1864):
            if p1[i][j]>230:
                a=1
            else:
                a=0
            if p2[i][j]>230:
                b=1
            else:
                b=0
            if a==b==1:
                one+=1
            else :
                if a!=b:
                    diff+=1
    similarity=1-diff/((1344-55)*(1864-133)-one)
    return similarity
def DrawImage(matrix,fn):
    x=[]
    y=[]
    for r in matrix:
        if r[0]<=-60.0 and r[0]>=-170.0 and r[1]<=70.0 and r[1]>=15.0:
            x.append(r[0])
            y.append(r[1])
    # print fn,len(x),len(y)
    plt.scatter(x,y,s=1,c="b",marker='.')
    # plt.yticks([xt for xt in range(15,70,5)])
    # plt.xticks([yt for yt in range(-170,-60,10)])
    plt.ylim(15,70)
    plt.xlim(-170,-60)
    plt.savefig(fn,bbox_inches='tight',dpi=350)
    plt.close()
def GetCoordinate(tb,keyword,limit,orderby=False):
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    sql=" select coordinate[0],coordinate[1] from "+tb+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
    if orderby:
        sql+=" order by id"
    if limit>=0:
        sql+=" limit "+str(limit)
    cur.execute(sql)
    return cur.fetchall()
def GetKeywords(tb,lower,upper,k):
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    sql="select vector,count from "+tb+" where count>="+str(lower)+" and count<"+str(upper)+"order by count limit "+str(k) #+" and vector not in (select distinct keyword from keyword_k_q) order by count"
    cur.execute(sql)
    return cur.fetchall()
#start 5000,3820000
def main(s,e):
    start=int(s)
    end=int(e)
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    keywords=GetKeywords('vectorcount',start,end)
    if len(keywords) > 0:
        for keyword in keywords:
            coord = GetCoordinate('coordtweets', keyword[0], -1)  # create perfect image of the file
            if len(coord)<=0:#some stop words may have no returns
                continue
            dirName = "FindingK/"# + str(freq) + "_" + str(freq + step) + "/"
            try:
                os.stat(dirName)
            except:
                os.mkdir(dirName)
            t1=time.time()
            perfectImageName=dirName+keyword[0]+"_perfect.png"
            DrawImage(coord,perfectImageName)
            perfectImage=GetGrayImage(perfectImageName)
            for i in [1,2,3,5,10,15,20,25,30,35,40,45,50,60,70,80,90]:
                k=i*keyword[1]/100
                kcoord=coord[0:k]
                kImageName=dirName+keyword[0]+"_"+str(k)+".png"
                DrawImage(kcoord,kImageName)
                kImage=GetGrayImage(kImageName)
                similarity=GetSililarityFromImages(perfectImage,kImage)
            t2=time.time()
            print keyword[0],t2-t1
                # sql="insert into keyword_r_q values('"+keyword[0]+"',0.71,"+str(similarity)+",'biased')"
                # cur.execute(sql)
                # cur.execute("commit")
                # print keyword,k,similarity
def perfectImageLenofKeyword(w):
    i=0
    j=0
    xStep=(x1-x0)/res_x
    yStep=(y1-y0)/res_y
    ilen=0
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sql="select * from tweets where "+box+"@>coordinate and to_tsvector('english',text)@@to_tsquery('english','"+w+"') limit 1"
            time1=time.time()
            cur.execute(sql)
            if len(cur.fetchall())>0:
                ilen+=1
            print time.time()-time1,x,y
    return ilen
# def seqSearchK():
#     t1=time.time()
#     for i in range(9,101):
#         k=i*len(ar)/100
#         Hs,xs,ys=np.histogram2d(ar[:k][:,0],ar[:k][:,1],bins=(res_x,res_y),range=((-170,-60),(15,70)))
#         sampleLen=np.count_nonzero(Hs)
#         similarity=float(sampleLen)/perfectLen
#         if similarity>0.85:
#             # sql = "insert into keyword_r_q values('" + keyword[0] + "'," + str(i) + "," + str(
#             #     similarity) + ",'100M')"
#             # cur.execute(sql)
#             # cur.execute("commit")
#             t2=time.time()
#             print "seq search time:",keyword[0],similarity,i,time.time()-t1
#             break
def hashByNumpy(ar,r=((-170,-60),(15,70))):
    H,x,y=np.histogram2d(ar[:,0],ar[:,1],bins=(res_x,res_y),range=r)
    return H
def imageLen(array):
    return np.count_nonzero(hashByNumpy(array))
def findkofQ(ar,Q):
    perfectLen=imageLen(np.array(ar))
    i=0.0
    l=0.0
    h=100.0
    similarity=0.0
    iterTimes=0
    while (similarity<0.85 or similarity>0.86) and iterTimes<10:
        if similarity<0.85:
            l=i
            i=(h+i)/2
        else:
            h=i
            i=(i+l)/2
        k=int(i*len(ar)/100)
        sampleLen=imageLen(np.array(ar[:k]))
        similarity=float(sampleLen)/perfectLen
        iterTimes+=1
    return i

def BinarySearch(w,q,tb,hybridtab='null'):
    coord=GetCoordinate(tb, w, -1)
    if len(coord)<5000:
        return 0

    offlineHs=np.zeros(shape=(480,270),dtype=int)
    if hybridtab is not 'null':
        offlinecoord=GetCoordinate(hybridtab,w,-1)
        offlineHs=hashByNumpy(np.array(offlinecoord))

    ar=np.array(coord) # create perfect image of the file
    H=hashByNumpy(ar)
    perfectLen=np.count_nonzero(H)
    i=0.0
    l=0.0
    h=100.0
    similarity=0.0
    iterTimes=0
    while (similarity<q or similarity>q*1.05) and iterTimes<20:
        if similarity<q:
            l=i
            i=(h+i)/2
        else:
            h=i
            i=(i+l)/2
        k=int(i*len(ar)/100)
        Hs=hashByNumpy(ar[:k])
        if hybridtab is not 'null':
            Hs+=offlineHs
        sampleLen=np.count_nonzero(Hs)
        similarity=float(sampleLen)/perfectLen
        iterTimes+=1
    return k
def myPerceptualHash(s,e,table):
    start=int(s)
    end=int(e)
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    keywords=GetKeywords('vectorcount',start,end,2000)
    t=time.time()
    if len(keywords) > 0:
        for keyword in keywords:
            t0=time.time()
            ar=np.array(GetCoordinate(table, keyword[0], -1)) # create perfect image of the file
            if len(ar)<=5000:#some stop words may have no returns
                continue
            t1=time.time()
            H=hashByNumpy(ar)
            perfectLen=np.count_nonzero(H)
            i=0.0
            l=0.0
            h=100.0
            similarity=0.0
            iterTimes=0
            t2=time.time()
            while (similarity<0.85 or similarity>0.86) and iterTimes<10:
                if similarity<0.85:
                    l=i
                    i=(h+i)/2
                else:
                    h=i
                    i=(i+l)/2
                k=int(i*len(ar)/100)
                Hs=hashByNumpy(ar[:k])
                sampleLen=np.count_nonzero(Hs)
                similarity=float(sampleLen)/perfectLen
                iterTimes+=1
            print keyword[0],"simlarity:",similarity,"ratio:",i,"fetch:",t1-t0,"draw:",t2-t1,"search:",time.time()-t2
    print "Total time of",table,":",time.time()-t
def switchExePlan():
    # pCoord=np.array(GetCoordinate('coordtweets','job',-1))
    pLen=7510
    for k in range(60000,60100):
        sCoord=np.array(GetCoordinate('coordtweets','job',k))
        sLen=np.count_nonzero(hashByNumpy(sCoord))
        print k,float(sLen)/pLen
def consistentTest():
    # a=np.array(GetCoordinate('coordtweets','job',-1))
    pLen=7510
    for i in range(35670,50000,1000):
        b=np.array(GetCoordinate('coord_sorted_tweets','job',i))
        sLen=np.count_nonzero(hashByNumpy(b))
        print float(sLen)/pLen,i
def planxchg():
    pLen=7510
    for i in range(22000,62651,2000 ):
        cur.execute("select * from tweets limit 11000000")
        t1=time.time()
        iRec=np.array(GetCoordinate('coord_sorted_tweets','job',i))
        t2=time.time()
        sLen=np.count_nonzero(hashByNumpy(iRec))
        print i,t2-t1, float(sLen)/pLen
def qualityofrange():
    pLen=7510
    r42500=GetCoordinate('coord_sorted_tweets','job',42500)#using sequential scan
    sLen1=np.count_nonzero(hashByNumpy(np.array(r42500)))
    print float(sLen1)/pLen
    for i in range(2500,42501,2500):
        ar=np.array(r42500[:i])
        sLen=np.count_nonzero(hashByNumpy(ar))
        print i,float(sLen)/pLen
def isSubsetFromFile(f1,f2):# f1 is a subset of f2
    ar1=np.genfromtxt(f1,delimiter=',')
    ar2=np.genfromtxt(f2,delimiter=',')
    for c in ar1:
        if c not in ar2:
            print False
            break
        print True
def isSubsetFromDB():
    ar1=np.array(GetCoordinate('coord_sorted_tweets','job',42500))
    for i in range(1,21):
        print i
        ar2=np.array(GetCoordinate('coord_sorted_tweets','job',43000))
        for c in ar2:
            if c not in ar1:
                print i, False
                break
def qualityChange():
    pLen=imageLen(np.array(GetCoordinate('coordtweets','love',-1)))
    for i in range(1,31):
        ar2=np.array(GetCoordinate('coordtweets','love',150000))
        iLen=np.count_nonzero(hashByNumpy(ar2))
        print float(iLen)/pLen
def orderbyTime():
    for k in range(50000,300000,10000):
        t1=time.time()
        GetCoordinate('coord_sorted_tweets','job',k)
        t2=time.time()
        GetCoordinate('coord_sorted_tweets','job',k,True)
        t3=time.time()
        print k,t2-t1,t3-t2
def ScaleDataSize():
    for w in ['soccer','beach','love']:
        coord=GetCoordinate('coordtweets',w,-1)
        print w, len(coord)
        for s in range(10,101,10):
            size=s*len(coord)/100
            scoord=coord[:size]
            r=findkofQ(scoord,0.85)
            print size,r*size
def m(n,p,l):
    # return math.factorial(n)*math.factorial(p)*math.pow(l,(n-l))/(math.factorial(l)*math.factorial(n-l)*math.factorial(l)*math.factorial(p-l))
    return math.factorial(p)*math.factorial(n-1)/(math.factorial(l)*math.factorial(p-l)*math.factorial(l-1)*math.factorial(n-l))
    # return math.factorial(n)*math.factorial(p)*math.pow(l,(n-l))/(math.factorial(n-l)*math.factorial(p-l))
# ScaleDataSize()
def prob(n,p,x):
    all=0
    sub=0
    for i in range(1,min(n,p)+1):
        all+=m(n,p,i)
        if i<=x:
            sub=all
    print float(sub)/all
def loadStatePolygon():
    poly=demjson.decode_file("state.json")
    for state in poly['features']:
        name=state['properties']['name']
        polys=state['geometry']['coordinates']
        for p in polys:
            coords="'"+str(p).replace('[','(').replace(']',')')[1:-1]+"'"
            sql="insert into statepolygon values('"+name+"',"+coords+")"
            cur.execute(sql)
            print name
    cur.execute('commit')
def updateStateField():
    cur.execute("select id from coordtweets")
    ids=cur.fetchall()
    i=0
    for id in ids:
        name="NULL"
        cur.execute("select state from statepolygon where poly@>(select coordinate from coordtweets where id="+str(id[0])+") limit 1")
        res=cur.fetchall()
        if len(res)>0:
            name=res[0][0]
        cur.execute("update coordtweets set state='"+name+"' where id="+str(id[0]))
        i+=1
        print i,id
    cur.execute('commit')
def countMap(w,k=4000000):
    sql="select state, count(*) from (select state,id from coordtweets where to_tsvector('english',text)@@to_tsquery('english','"+w+"') limit "+str(k)+") t group by t.state"
    cur.execute(sql)
    return cur.fetchall()
def countMapQuality(s,e):
    keywords=GetKeywords('vectorcount',s,e,1000)
    for w in keywords:
        gt=dict((x,y) for x,y in countMap(w[0]))
        for i in gt.keys():
            gt[i]=float(gt[i])/w[1]
        for r in range(1,101,1):
            k=r*w[1]/100
            sub=dict((x,y) for x,y in countMap(w[0],k))
            for i in sub.keys():
                sub[i]=float(sub[i])/k
            e=0.0
            for i in gt.keys():
                if sub.has_key(i):
                    e+=math.pow((gt[i]-sub[i]),2)
                else:
                    e+=math.pow(gt[i],2)
            print w[0],r/1000,k, math.sqrt(e)
def getError(gt,freq,sub,k):
    e=0.0
    for i in gt.keys():
        if sub.has_key(i):
            e+=math.pow(float(gt[i])/freq-float(sub[i])/k,2)
        else:
            e+=math.pow(float(gt[i])/freq,2)
    return math.sqrt(e)
def countMapQualityMem(s,e):
    keywords=GetKeywords('vectorcount',s,e,1000)
    for w in keywords:
        cur.execute("select state from coordtweets where to_tsvector('english',text)@@to_tsquery('english','"+w[0]+"')")
        res=cur.fetchall()
        freq=len(res)
        gt={}
        for i in res:
            if gt.has_key(i[0]):
                gt[i[0]]+=1
            else:
                gt[i[0]]=1
    for r in range(1,201,1):
            k=r*w[1]/1000
            sub={}
            for i in range(0,k):
                if sub.has_key(res[i][0]):
                    sub[res[i][0]]+=1
                else:
                    sub[res[i][0]]=1
            print w[0],float(k)/w[1],getError(gt,freq,sub,k)
def kkk(s,e):
    keywords=GetKeywords('vectorcount',s,e,1000)
    online=list()
    offset0=list()
    offset50=list()
    offsetalpha=list()
    for w in keywords:
        online.append(BinarySearch(w[0],0.85,'coord_sorted_tweets'))
        offset0.append(BinarySearch(w[0],0.85,'coord_sorted_tweets','gridsample0'))
        offset50.append(BinarySearch(w[0],0.85,'coord_sorted_tweets','gridsample50'))
        offsetalpha.append(BinarySearch(w[0],0.85,'coord_sorted_tweets','gridsample'))
    for i in range(0,len(w)):
        print w[i][0],w[i][1],online[i],offset0[i],offset50[i],offsetalpha[i]
kkk(100000,1000000)
