import psycopg2
import cv2 as cv
import matplotlib.pyplot as plt
import os
import time
import math
import numpy as np

res_x=1920/4
res_y=1080/4

y0=15.0
y1=70.0

x0=-170.0
x1=-60.0
conStr="dbname='postgres' user='postgres' host='169.234.57.197' password='liming' "
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
def GetCoordinate(tb,keyword,limit):
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    sql=" select coordinate[0],coordinate[1] from "+tb+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
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
    k=42670
    # a=np.array(GetCoordinate('coordtweets','job',-1))
    pLen=7510
    for i in range(1,21):
        b=np.array(GetCoordinate('coordtweets','job',k))
        sLen=np.count_nonzero(hashByNumpy(b))
        print float(sLen)/pLen
consistentTest()
# switchExePlan()
# main(sys.argv[1],sys.argv[2])
# myPerceptualHash(10,20)
# myPerceptualHash(1000,1050)
# myPerceptualHash(10000,10500)
# myPerceptualHash(100000,105000)
# myPerceptualHash(5000,4000000,'coordtweets')
# myPerceptualHash(5000,4000000,'tweets')
# myPerceptualHash(1000000,1900000)
# main(5000,4000000)