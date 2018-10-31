import math
import time
import numpy as np
import json
import datetime
import LIMITlib as lmt
from numpy import genfromtxt

# ary=[[1,1],[2,2],[3,3],[2.1,2.1],[3.2,3.2]]
# print VAS(ary,4)
#id, x, y, resp
def doVasFile(file):
    ary=genfromtxt(file,delimiter=',')
    b=np.zeros(len(ary))
    ary=np.insert(ary,3,values=b,axis=1)
    ######
    print "doing vas", len(ary)
    sampleList = VAS(ary, len(ary) * 0.03)
    return


def doVas(array):
    VAS(array, len(array) * 0.01)


def VAS(PointArray, SampleSize):
    SampleList = []
    i=0;
    start=datetime.datetime.now()
    for point in PointArray:
        i+=1
        if i%10000==0:
            print i,"time elapsed:",(datetime.datetime.now()-start).seconds,"s"
        if len(SampleList) < SampleSize:
            Expand(SampleList, point)
        else:
            Expand(SampleList, point)
            Shrink(SampleList)
    return SampleList

def VASDb():
    sql="select id,coordinate[0],coordinate[1] from rnd5 offset 547947"
    lmt.cur.execute(sql)
    restRec=lmt.cur.fetchall()
    i=0
    t1=time.time()
    for rec in restRec:
        ExpandDb(rec)
        ShrinkDb()
        i+=1
        print i
    lmt.cur.execute("commit")
    t2=time.time()
    print t1-t2

def ExpandDb(rec):
    sql="update vas5 set delta=exp(-(coordinate<->point("+str(rec[1])+","+str(rec[2])+"))) where (coordinate<->point("+str(rec[1])+","+str(rec[2])+"))<4"
    lmt.cur.execute(sql)
    #lmt.cur.execute("commit")
    lmt.cur.execute("update vas5 set resp=resp+delta  where (coordinate<->point("+str(rec[1])+","+str(rec[2])+"))<4")
    #lmt.cur.execute("commit")
    lmt.cur.execute("select sum(delta) from vas5  where (coordinate<->point("+str(rec[1])+","+str(rec[2])+"))<4")
    resp=lmt.cur.fetchall()[0][0]
    sql="insert into vas5(id,coordinate,resp,delta) values('"+str(rec[0])+"',point("+str(rec[1])+","+str(rec[2])+"),"+str(resp)+",0)"
    lmt.cur.execute(sql)
       #lmt.cur.execute("commit")

def Expand(SampleList, Point):
    rsp = 0
    for p in SampleList:
        dist = Kappa(Point, p)
        p[3] += dist
        rsp += dist
    SampleList.append([Point[0], Point[1],Point[2],rsp])
    return


def Shrink(SampleList):
    maxRsp = 0
    index = 0
    for i in range(0, len(SampleList)):
        if SampleList[i][3] > maxRsp:
            maxRsp = SampleList[i][3]
            index = i
    for i in range(0, len(SampleList)):
        if i != index:
            SampleList[i][3] -= Kappa(SampleList[index], SampleList[i])
    del SampleList[index]
    return
def ShrinkDb():
    sql="select id,coordinate[0],coordinate[1] from vas5 order by resp desc limit 1"
    lmt.cur.execute(sql)
    rec=lmt.cur.fetchall()
    sql="update vas5 set delta=exp(-(coordinate<->point("+str(rec[0][1])+","+str(rec[0][2])+"))) where (coordinate<->point("+str(rec[0][1])+","+str(rec[0][2])+"))<4"
    lmt.cur.execute(sql)
    #lmt.cur.execute("commit")
    lmt.cur.execute("update vas5 set resp=resp-delta where (coordinate<->point("+str(rec[0][1])+","+str(rec[0][2])+"))<4")
    lmt.cur.execute("delete from vas5 where id="+str(rec[0][0]))
    #lmt.cur.execute("commit")

def Kappa(x, y):
    distSquare = (x[1] - y[1]) * (x[1] - y[1]) + (x[2] - y[2]) * (x[2] - y[2])
    return math.exp(-distSquare)

#VASDb()
doVasFile("../../rnd5.csv")
