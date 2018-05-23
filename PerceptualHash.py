import cv2 as cv
from os import listdir

def dhash(image):
    x=819
    y=879
    resized=cv.resize(image,(x,y))
    ret=""
    for i in range(0,y):
        for j in range(0,x):
            if resized[i][j]>230:
                ret+='1'
            else:
                ret+='0'
    return ret
files=listdir('image/city')
for f in range(0,len(files)):
    image1=cv.imread('image/city/'+files[f])
    image1=cv.cvtColor(image1,cv.COLOR_RGB2GRAY)
    p1=dhash(image1)
    for s in range(f+1,len(files)):
        image2=cv.imread('image/city/'+files[s])
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
        print files[f],files[s],similarity