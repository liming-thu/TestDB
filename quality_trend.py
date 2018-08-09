### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
import FindingK as fk
def DrawImage(ar):
    x=np.arange(1,101,5,dtype=int)

    plt.figure(figsize=(5,3))
    plt.xlabel('k/f(%)')
    plt.ylabel('quality(%)')
    plt.ylim(0,100)

    # plt.plot(x,ar[6]*100,marker='v')
    plt.plot(x,ar[5]*100,marker='+')
    plt.plot(x,ar[4]*100,marker='o')
    plt.plot(x,ar[3]*100,marker='x')
    plt.plot(x,ar[2]*100,marker='d')
    plt.plot(x,ar[1]*100,marker='s')
    plt.plot(x,ar[0]*100,marker='*')

    plt.legend(['job','healthcare','rain','beach','home','soccer'])
    plt.grid(True)
    plt.show()
def qualities(keywords):
    step=5
    q=np.zeros(shape=(len(keywords),100/step))
    for i in range(0,len(keywords)):
        coord=np.array(fk.GetCoordinate('coordtweets',keywords[i][0],-1))
        pLen=fk.imageLen(coord)
        for j in range(1,101,5):
            k=j*keywords[i][1]/100
            sLen=fk.imageLen(coord[:k])
            q[i][j/step]=float(sLen)/pLen
    return q
wr0=fk.GetKeywords('vectorcount',5418,5419,1)#soccer
wr1=fk.GetKeywords('vectorcount',94803,94804,1)#home
wr2=fk.GetKeywords('vectorcount',104583,104584,1)#beach
wr3=fk.GetKeywords('vectorcount',106875,106877,1)#rain
wr4=fk.GetKeywords('vectorcount',210000,211000,1)#healthcare
wr5=fk.GetKeywords('vectorcount',3000000,3200000,1)#job

w=wr0+wr1+wr2+wr3+wr4+wr5
print w
q=qualities(w)
DrawImage(q)