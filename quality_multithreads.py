### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
import FindingK as fk
def DrawImage(ar):
    x=np.arange(1,26,1,dtype=int)

    plt.figure(figsize=(5,3))
    plt.xlabel('the (i)th run')
    plt.ylabel('quality(%)')
    plt.ylim(40,100)
    # plt.xlim(54,100)

    plt.plot(x,ar[0]*100,marker='+')
    plt.plot(x,ar[1]*100,marker='o')
    plt.plot(x,ar[2]*100,marker='v')
    plt.plot(x,ar[3]*100,marker='x')
    plt.plot(x,ar[4]*100,marker='d')

    plt.legend(range(15000,55001,10000))
    plt.grid(True)
    plt.show()

def qualityChange():
    pLen=fk.imageLen(np.array(fk.GetCoordinate('coordtweets','market',-1)))
    print pLen
    ar=np.zeros(shape=(5,25))
    for i in range(0,5):
        for j in range(0,25):
            iLen=fk.imageLen(np.array(fk.GetCoordinate('coordtweets','market',15000+10000*i)))
            ar[i][j]=float(iLen)/pLen
            print i,j,ar[i][j]
    DrawImage(ar)
qualityChange()