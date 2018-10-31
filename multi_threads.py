### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
import LIMITlib as fk
def DrawImage(ar):
    x=np.arange(1,19,1,dtype=int)

    plt.figure(figsize=(5,3))
    plt.xlabel('the (i)th run')
    plt.ylabel('quality(%)')
    plt.ylim(40,100)
    # plt.xlim(54,100)

    # plt.plot(x,ar[4]*100,marker='+')
    # plt.plot(x,ar[3]*100,marker='o')
    # plt.plot(x,ar[2]*100,marker='v')
    # plt.plot(x,ar[1]*100,marker='x')
    # plt.plot(x,ar[0]*100,marker='d')

    plt.errorbar(x,ar[4]*100,yerr=0.23,marker='+',capsize=3,elinewidth=1)
    # plt.errorbar(x,ar[3]*100,yerr=0.33*10,marker='o')
    # plt.errorbar(x,ar[2]*100,yerr=0.43*10,marker='v')
    # plt.errorbar(x,ar[1]*100,yerr=0.53*10,marker='x')
    plt.plot(x,ar[3]*100,marker='o',linewidth=1)
    plt.plot(x,ar[2]*100,marker='v',linewidth=1)
    plt.plot(x,ar[1]*100,marker='x',linewidth=1)
    plt.errorbar(x,ar[0]*100,yerr=0.60,marker='d',capsize=3,elinewidth=1)

    plt.legend(range(55001,15000,-10000))
    plt.grid(True)
    plt.show()

def qualityChange():
    pLen=fk.imageLen(np.array(fk.GetCoordinate('rnd5','market',-1)))
    print pLen
    ar=np.zeros(shape=(5,18))
    for i in range(0,5):
        for j in range(0,18):
            iLen=fk.imageLen(np.array(fk.GetCoordinate('rnd5','market',15000+10000*i)))
            ar[i][j]=float(iLen)/pLen
            print i,j,ar[i][j]
    #DrawImage(ar)
qualityChange()
