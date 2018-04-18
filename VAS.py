import math
import numpy as np
import json
import threading
# ary=[[1,1],[2,2],[3,3],[2.1,2.1],[3.2,3.2]]
# print VAS(ary,4)

def doVasFile(file):
    print file
    f=open("state/"+file)
    ln=f.readline().replace("None,","")
    f.close()
    ary=np.asarray(json.loads(ln))
    VAS(ary,len(ary)*0.01)
    return
def doVas(array):
    VAS(array,len(array)*0.01)
def VAS(PointArray,SampleSize):
    SampleList=[]
    for point in PointArray:
        if len(SampleList)<SampleSize:
            Expand(SampleList,point)
        else:
            Expand(SampleList,point)
            Shrink(SampleList)
    return SampleList

def Expand(SampleList,Point):
    rsp=0
    for p in SampleList:
        dist=Kappa(Point,p)
        p[2]+=dist
        rsp+=dist
    SampleList.append([Point[0],Point[1],rsp])
    return
def Shrink(SampleList):
    maxRsp=0
    index=0
    for i in range(0,len(SampleList)):
        if SampleList[i][2]>maxRsp:
            maxRsp=SampleList[i][2]
            index=i
    for i in range(0,len(SampleList)):
        if i!=index:
            SampleList[i][2]-=Kappa(SampleList[index],SampleList[i])
    del SampleList[index]
    return

def Kappa(x,y):
    distSquare=(x[0]-y[0])*(x[0]-y[0])+(x[1]-y[1])*(x[1]-y[1])
    return math.exp(-distSquare)
# doVasFile("1.dat")