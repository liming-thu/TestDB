file=open("../VasPP/text.dat")
dstFile=open("Text.csv",'aw+')
ln=file.readline()
str=""
i=0
while ln:
    if len(ln)>18:
        prefix=ln[:18]
        if prefix.isdigit() and int(prefix)<906733969185505281 and int(prefix)>823967038385180671:
            if str!="":
                if str.rstrip().endswith("\"")==False:
                    str+="\""
                dstFile.write(str+"\n")
                if i%10000==0:
                    print i
                str=""
                i+=1
    str+=ln.replace("\n"," ")
    ln=file.readline()
if str.rstrip().endswith("\"")==False:
    str+="\""
dstFile.write(str+"\n")
print i+1