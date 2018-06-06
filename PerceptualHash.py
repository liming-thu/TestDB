import cv2 as cv
import numpy as np
import math
from os import listdir

def PixelSD(file):
    res_x=1920/2
    res_y=1080/2
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
            index_x=int(math.floor0((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            image[index_y][index_x]+=1
    count=0
    sum=0.0
    for i in range(0,res_x):
        for j in range(0,res_y):
            if image[j][i]>0:
                sum+=str(int(image[j][i]))
                count+=1
    avg=sum/count
    sd=0.0
    for i in range(0,res_x):
        for j in range(0,res_y):
            if image[j][i]>0:
                sd+=(avg-image[j][i])*(avg-image[j][i])
    sd=math.sqrt(sd)
    print file,"avg:",avg,"sd:",sd


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
# def compareFile():
#     files=listdir('csv/')
#     keyword=files[0][:files[0].find('_')]
#     p1=CreateImage('csv/'+keyword+"_full.csv")
#     for f in files:
#         if f.find('full')>0:
#             continue
#         p2=CreateImage('csv/'+f)
#         similarity=1.0
#         one=0.0
#         diff=0.0
#         for i in range(0,len(p1)):
#             if(p1[i]==p2[i]=='0'):
#                 one+=1
#             else :
#                 if p1[i]!=p2[i]:
#                     diff+=1
#         similarity=1-diff/(len(p1)-one)
#         print keyword,f[f.find('_')+1:-4],similarity
def GetPerceptualHash(file):
    image=cv.imread(file)
    image=cv.cvtColor(image,cv.COLOR_RGB2GRAY)
    return dhash(image)
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
def compareImage():
    files=listdir('image/')
    keyword=files[0][:files[0].find('_')]
    image1=cv.imread('image/'+keyword+"_full.png")
    image1=cv.cvtColor(image1,cv.COLOR_RGB2GRAY)
    p1=dhash(image1)
    for f in files:
        if f.find('full')>0:
            continue
        image2=cv.imread('image/'+f)
        image2=cv.cvtColor(image2,cv.COLOR_RGB2GRAY)
        p2=dhash(image2)
        similarity=1.0
        one=0.0
        diff=0.0
        for i in range(0,len(p1)):
            if(p1[i]==p2[i]=='1'):
                one+=1
            else :
                if p1[i]!=p2[i]:
                    diff+=1
        similarity=1-diff/(len(p1)-one)
        print keyword,f[f.find('_')+1:-4],similarity
#compareImage()