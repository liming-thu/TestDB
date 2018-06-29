import psycopg2
import numpy as np
import time
import math
from pyemd import emd,emd_samples

res_x=1920/20
res_y=1080/20

x0=15.0
x1=70.0

y0=-170.0
y1=-60.0
conStr="dbname='postgres' user='postgres' host='169.234.57.197' password='liming' "
def MatrixFromCoordinates(coord):
    pixel_x=(x1-x0)/res_x
    pixel_y=(y1-y0)/res_y
    matrix=np.zeros(shape=(res_x,res_y))
    for c in coord:
        y=float(c[0])
        x=float(c[1])
        if y<y0 or y>y1 or x>x1 or x<x0:
            continue
        else:
            index_x=int(math.floor((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            matrix[index_x][index_y]+=1
            # matrix[index_x][index_y]=1
    return matrix
def myEMD(m1,m2):
    distMatrix=np.zeros(shape=(len(m1),len(m1[0]),len(m2),len(m2[0])))
    for i1 in range(0,len(m1)):
        for j1 in range(0,len(m1[0])):
            for i2 in range(0,len(m2)):
                for j2 in range(0,len(m2[0])):
                    distMatrix[i1][j2][i2][j2]=(i1-i2)*(i1-i2)+(j1-j2)*(j1-j2)
    m1=m1.flatten()
    m2=m2.flatten()
    distMatrix=distMatrix.reshape(len(m1),len(m2))
    d=emd(m1,m2,distMatrix)
    return d
def GetCoordinate(tb,keyword,limit):
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    sql="select coordinate[0],coordinate[1] from "+tb+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
    if limit>0:
        sql+=" limit "+str(limit)
    cur.execute(sql)
    return cur.fetchall()
def GetKeywords(tb,lower,upper):
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    sql="select vector,count from "+tb+" where count>="+str(lower)+" and count<"+str(upper)#+" and vector not in (select distinct keyword from keyword_k_q) order by count"
    cur.execute(sql)
    return cur.fetchall()
def myPerceptualHash(s,e):
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
            perfectMatrix=MatrixFromCoordinates(coord)
            for i in range(0,101):
                t1=time.time()
                curK=i*keyword[1]/100
                sampleMatrix=MatrixFromCoordinates(coord[:curK])
                similarity=myEMD(perfectMatrix,sampleMatrix)
                t2=time.time()
                print keyword[0],i,similarity,t2-t1,'s'
myPerceptualHash(56000,81000)
