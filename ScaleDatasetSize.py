import LIMITlib as lmt
import numpy as np

def scale():
    kwList=["soccer","music","traffic","veteran"]
    lmt.ScaleDataSize(kwList,'tweets')
def mse(w,binary=True):
    coord=lmt.GetCoordinate('tweets',w)
    perfectImage=lmt.hashByNumpy(np.array(coord))
    print "heat map:"
    print "base:",lmt.myMSE(perfectImage,lmt.hashByNumpy(np.zeros((2,2))),binary)
    for r in range(10,100,10):
        l=int(float(r)*len(coord)/100.0)
        aprxImage=lmt.hashByNumpy(np.array(coord[:l]))
        print r,lmt.myMSE(perfectImage,aprxImage,binary)
def ph():
    coord=lmt.GetCoordinate('tweets','soccer')
    perfectImage=lmt.imageLen(np.array(coord))
    for r in range(10,100,10):
        l=int(float(r)*len(coord)/100.0)
        aprxImage=lmt.imageLen(np.array(coord[:l]))
        print r,float(aprxImage)/perfectImage
mse('week',False)
mse('week',True)
ph()