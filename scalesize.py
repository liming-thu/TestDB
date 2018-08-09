### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
import FindingK as fk
def DrawImage(ar):
    x=np.arange(1,11,1,dtype=int)

    y=np.arange(10000000,100000001,10000000,dtype=int)
    plt.figure(figsize=(5,3))
    plt.xlabel('Dataset')
    plt.ylabel('#Records/k')
    plt.ylim(0,100050000)
    plt.xlim(0,11)
    plt.plot(x,ar[0],marker='+')
    plt.plot(x,ar[1],marker='o')
    plt.plot(x,ar[2],marker='v')
    plt.plot(x,y,marker='d')

    plt.legend(['job','beach','love','dataset'])
    plt.grid(True)
    plt.show()

def main():
    ar=np.zeros(shape=(3,10),dtype=float)
    i=0
    for w in ['job','love','beach','soccer']:
        coord=fk.GetCoordinate('tweets',w,-1)
        print w, len(coord)
        j=0
        for s in range(20,101,20):
            size=s*len(coord)/100
            scoord=coord[:size]
            r=fk.findkofQ(scoord,0.85)
            # ar[i][j]=r*size/100
            j+=1
            print w,r*size/100
        i+=1
    return ar
# DrawImage(main())
main()