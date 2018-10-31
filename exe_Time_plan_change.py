### the executiom itme when increasing k, plan change
import matplotlib.pyplot as plt
import numpy as np
def DrawImage():
    # coord=np.genfromtxt('4w3_random_index_scan.csv',delimiter=',')
    # coord2=np.genfromtxt('4w25_random_seq_scan.csv',delimiter=',')
    # plt.scatter(coord2[:,0],coord2[:,1],s=1,c="b",marker='.')
    p1=np.genfromtxt('p.csv',delimiter=',')
    p2=np.genfromtxt('quality_plan_change.csv',delimiter=',')

    # plt.xlabel('k')
    # plt.ylabel('quality(%)')

    fig=plt.figure(figsize=(5,2.7))

    ax1 = fig.add_subplot(111)
    ax1.plot(p1[:,0],p1[:,1],'blue',marker='.',mfc='red')
    ax1.set_ylabel('time(s)')
    ax1.set_xlabel("k")
    # ax1.set_title('time')
    # ax1.set_title("Double Y axis")

    ax2 = ax1.twinx()  # this is the important function
    ax2.plot(p2[:,0],p2[:,1]*100, 'r',marker='.',mfc='blue')
    ax2.set_ylabel('quality(%)')
    ax2.set_xlabel('k')
    # ax2.set_title('quality')

    xlmin=np.min(p1[:,0])-0.1*(np.max(p1[:,0])-np.min(p1[:,0]))
    xlmax=np.max(p1[:,0])+0.1*(np.max(p1[:,0])-np.min(p1[:,0]))
    y1lmin=np.min(p1[:,1])-0.1*(np.max(p1[:,1])-np.min(p1[:,1]))
    y1lmax=np.max(p1[:,1])+0.1*(np.max(p1[:,1])-np.min(p1[:,1]))
    y2lmin=np.min(100*(p2[:,1])-0.1*(np.max(p2[:,1])-np.min(p2[:,1])))
    y2lmax=np.max(100*(p2[:,1])+0.1*(np.max(p2[:,1])-np.min(p2[:,1])))
    ax1.set_xlim(xlmin,xlmax)
    ax2.set_ylim(y2lmin,y2lmax)
    ax1.set_ylim(y1lmin,y1lmax)

    ax1.legend(['time'],loc='upper left')
    ax2.legend(['quality'],loc='upper right')

    plt.grid(True)
    plt.show()

DrawImage()