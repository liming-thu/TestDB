### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import LIMITlib as fk
import seaborn
def DrawImage():
    # seaborn.set()
    plt.figure(figsize=(1.6,1.8))
    wlist=['work','job']
    k=[2.32,2.95]
    k1=[0.78,0.10]
    k0=[0.40,0.57]
    plt.ylim(0,3.3)
    x=list(range(len(wlist)))
    total_width,n=0.9,3
    width=total_width/n
    plt.yscale=1000
    b0=plt.bar(x,k,label='t',width=width,fc='y')
    for i in range(len(x)):
        x[i]=x[i]+width
    b1=plt.bar(x,k0,width=width,label='t0',tick_label=wlist,fc='b')
    for i in range(len(x)):
        x[i]=x[i]+width
    b2=plt.bar(x,k1,width=width,label='t1',fc='r')
    for b in b0+b1+b2:
        h=b.get_height()
        plt.text(b.get_x()+b.get_width()/2,h,'%1.2f'%h,ha='center',va='bottom')
    plt.legend()
    plt.show()
DrawImage()