### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
import LIMITlib as fk
def qualities(keywords):
    step=5
    q=np.zeros(shape=(len(keywords),100/step))
    for i in range(0,len(keywords)):
        coord=fk.GetCoordinate('rnd5',keywords[i],-1)
        print keywords[i],len(coord)
        matrix=np.array(coord)
        pLen=fk.imageLen(matrix)
        for j in range(1,101,5):
            k=int(float(j*len(coord))/100.0)
            sLen=fk.imageLen(np.array(coord[:k]))
            q[i][j/step]=float(sLen)/pLen
        print q
qualities(['soccer','home','beach','rain','healthcare','job'])
