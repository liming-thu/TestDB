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

    plt.plot(x,ar[0]*100,marker='+')
    plt.plot(x,ar[1]*100,marker='o')
    plt.plot(x,ar[2]*100,marker='v')
    plt.plot(x,ar[3]*100,marker='x')
    plt.plot(x,ar[4]*100,marker='d')
    plt.plot(x,ar[5]*100,marker='s')
    plt.plot(x,ar[6]*100,marker='*')

    plt.legend(range(1,len(ar)+1))
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
wr0=fk.GetKeywords('vectorcount',5418,5419,0)#soccer
wr1=fk.GetKeywords('vectorcount',5008,6000,3)#twitter,volunt,bout
wr2=fk.GetKeywords('vectorcount',104583,104584,0)#beach
wr3=fk.GetKeywords('vectorcount',106875,106877,0)#rain
wr4=fk.GetKeywords('vectorcount',210000,211000,1)#healthcare,manage
wr5=fk.GetKeywords('vectorcount',217000,221000,1)#sale
wr6=fk.GetKeywords('vectorcount',2000000,3200000,2)

w=wr0+wr1+wr2+wr3+wr4+wr5+wr6
q=qualities(w)
DrawImage(q)