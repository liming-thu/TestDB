from os import listdir
dict={'see':0};
i=0
for file in listdir('/root/TestDB/wc'):
    f=open('/root/TestDB/wc/'+file)
    for ln in f.readlines():
        str=ln.split()
        if dict.has_key(str[0]):
            dict[str[0]]+=int(str[1])
        else:
            dict[str[0]]=int (str[1])
        i+=1
        if i%10000==0:
            print file,i
    f.close()
print "total lines:",i
w=open('/root/TestDB/fullwc.dat','aw+')
for d in dict.keys():
    w.write(d+','+dict[d].__str__()+'\n')

