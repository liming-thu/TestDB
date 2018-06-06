import cv2 as cv
import numpy as np
import math
from os import listdir

def PixelSD(file):
    res_x=1920
    res_y=1080
    x0=15.0
    x1=70.0
    y0=-170.0
    y1=-60.0
    pixel_x=(x1-x0)/res_x
    pixel_y=(y1-y0)/res_y
    image=np.zeros((res_y,res_x))
    f=open(file)
    for ln in f.readlines():
        ln=ln.replace('(','').replace(')','').replace('\\','')
        y=float(ln.split(',')[0])
        x=float(ln.split(',')[1])
        if y<y0 or y>y1 or x>x1 or x<x0:
            continue
        else:
            index_x=int(math.floor((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            image[index_y][index_x]+=1
    count=0
    sum=0.0
    for i in range(0,res_x):
        for j in range(0,res_y):
            if image[j][i]>0:
                sum+=int(image[j][i])
                count+=1
    avg=sum/count
    sd=0.0
    for i in range(0,res_x):
        for j in range(0,res_y):
            if image[j][i]>0:
                sd+=(avg-image[j][i])*(avg-image[j][i])
    sd=math.sqrt(sd)
    print file,"avg:",avg,"sd:",sd

files=listdir('data/')
for file in files:
    PixelSD('data/'+file)