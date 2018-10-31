import math
import time
import numpy as np
import json
import datetime
import LIMITlib as lmt
from numpy import genfromtxt

# ary=[[1,1],[2,2],[3,3],[2.1,2.1],[3.2,3.2]]
# print VAS(ary,4)

def doVasFile(file):
    ary=genfromtxt(file,delimiter=',')
    b=np.zeros(len(ary))
    ary=np.insert(ary,3,values=b,axis=1)
    ary=np.insert(ary,3,values=b,axis=1)
    print ary[0]
    print "doing vas", len(ary)
    sampleList = VAS(ary, len(ary) * 0.03)
    np.save("vas.csv",sampleList[:,1])
    return


def VAS(PointArray, SampleSize):
    SampleList = PointArray[:int(SampleSize)]
    print "Sample List Size:",len(SampleList)
    i=0;
    start=datetime.datetime.now()
    for point in PointArray[int(SampleSize):]:
        i+=1
        if i%10000==0:
            print i,"time elapsed:",(datetime.datetime.now()-start).seconds,"s"
        Expand(SampleList, point)
        Shrink(SampleList)
    return SampleList

def Expand(SampleList, Point):
    a=SampleList[:,1]-Point[1]
    b=SampleList[:,2]-Point[2]
    a=a**2
    b=b**2
    c=np.sqrt(a+b)
    SampleList[:,4]=np.exp(-c)
    SampleList[:,3]+=SampleList[:,4]
    Point[3]=np.sum(SampleList[:,3])
    Point[4]=0
    SampleList=np.vstack(Point)

def Shrink(SampleList):
    row=np.argmax(SampleList[:,3])
    Point=SampleList[row]
    a=SampleList[:,1]-Point[1]
    b=SampleList[:,2]-Point[2]
    a=a**2
    b=b**2
    c=np.sqrt(a+b)
    SampleList[:,4]=np.exp(-c)
    SampleList[:,3]-=SampleList[:,4]
    SampleList=np.delete(SampleList, row, 0)

doVasFile("../../rnd5.csv")
