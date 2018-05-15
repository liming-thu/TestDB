file=open("Text.csv")
dstFile=open("pointText.csv",'aw+')
ln=file.readline()
str=""
i=0
while ln:
    if ln.rstrip().endswith(']\"'):
        cord=ln[-40:]
        cord=cord.replace('[','(').replace(']',')')
        ln=ln[:-40]+cord
        i+=1
        if i%10000==0:
            print i
    dstFile.write(ln)
    ln=file.readline()
print i
# dstFile.write(ln+"\n")