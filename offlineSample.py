import psycopg2
import numpy as np
import time
import math
import FindingK as fk

granularity=4

res_x=1920/granularity
res_y=1080/granularity

y0=15.0
y1=70.0
yStep=(y1-y0)/res_y

x0=-170.0
x1=-60.0
xStep=(x1-x0)/res_x

conStr="dbname='postgres' user='postgres' host='169.234.49.169' password='liming' "
conn=psycopg2.connect(conStr)
cur=conn.cursor()

def densityofFullDataset():
    f=open("10M.csv")
    image={}
    i=0
    for line in f.readlines():
        c = line[line.rfind("(") + 1:line.rfind(")")].split(",")
        pixel_x = (x1 - x0) / res_x
        pixel_y = (y1 - y0) / res_y
        x = float(c[0])
        y = float(c[1])
        if y < y0 or y > y1 or x > x1 or x < x0:
            continue
        else:
            index_x = int(math.floor((x - x0) / pixel_x))
            index_y = int(math.floor((y - y0) / pixel_y))
            key = str(index_x) + "," + str(index_y)
            if key not in image.keys():
                image[key] = 1
            else:
                image[key] += 1
        i += 1
        if i % 100000 == 0:
            print i
    print i
    f.close()
    f.open("fullds.csv",'aw+')
    for p in image:
        f.writelines(p+","+str(image[p])+","+image[0]+",full\n")

def GetCoordinate(tb,keyword,limit):
    sql="select coordinate[0],coordinate[1] from "+tb+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
    if limit>0:
        sql+=" limit "+str(limit)
    cur.execute(sql)
    return cur.fetchall()
def GetKeywords(tb,lower,upper):
    sql="select vector from "+tb+" where count>="+str(lower)+" and count<"+str(upper)+" order by count"#+" and vector not in (select distinct keyword from keyword_k_q) order by count"
    cur.execute(sql)
    return cur.fetchall()
def fineSample(s,e):
    keywords=GetKeywords('vectorcount',s,e)
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            for keyword in keywords:
                sql="insert into finesample select * from coordtweets where "+box+"@>coordinate and to_tsvector('english',text)@@to_tsquery('english','"+keyword[0]+"') limit 1"
                cur.execute(sql)
            print res_x,x,res_y,y
    cur.execute('commit')
#remove top n cells
def gridSampleTopCells(k,offset=0,alpha=0):
    i=0
    j=0
    for x in range(0,res_x):
        for y in range(0,res_y):
            tmpoffset=offset
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            #remove top n cells
            sql="select count(*) from biasedonepercent where "+box+"@>coordinate"
            cur.execute(sql)
            cnt=cur.fetchall()[0][0]
            if cnt>=k:
                continue
            else:
                tmpk=k-cnt
            sqlcnt="select count(*) from coordtweets where "+box+"@>coordinate"
            cur.execute(sqlcnt)
            cnt=cur.fetchall()
            if cnt[0][0]>=tmpk:
                tmpoffset=cnt[0][0]-tmpk
            else:
                tmpoffset=0
            if cnt[0][0]>0:
                sql="insert into gridsample select * from coordtweets where "+box+"@>coordinate offset "+str(tmpoffset)+" limit "+str(tmpk)
                cur.execute(sql)
                print res_x,x,res_y,y,cnt[0][0],tmpoffset,tmpk
    cur.execute('commit')
    print "Grid Sample: k="+str(k)
def gridSample(k,offset=0,alpha=0):
    i=0
    j=0
    dmax=399000
    # for x in range(0,res_x):
    #     for y in range(0,res_y):
    #         tmpoffset=offset
    #         bottomleftX=x0+xStep*x
    #         bottomleftY=y0+yStep*y
    #         toprightX=x0+xStep*(x+1)
    #         toprightY=y0+yStep*(y+1)
    #         box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
    #         sqlcnt="select count(*) from coordtweets where "+box+"@>coordinate"
    #         cur.execute(sqlcnt)
    #         cnt=cur.fetchall()
    #         if cnt[0][0]>dmax:
    #             dmax=cnt[0][0]
    for x in range(0,res_x):
        for y in range(0,res_y):
            tmpoffset=offset
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sqlcnt="select count(*) from coordtweets where "+box+"@>coordinate"
            cur.execute(sqlcnt)
            cnt=cur.fetchall()
            if cnt[0][0]==0:
                continue
            tmpk=int(k*float(max(0,dmax-alpha*cnt[0][0]))/dmax)
            if cnt[0][0]>=tmpk:
                tmpoffset=cnt[0][0]-tmpk
            else:
                tmpoffset=0
            sql="insert into gridsample select * from coordtweets where "+box+"@>coordinate offset "+str(tmpoffset)+" limit "+str(tmpk)
            cur.execute(sql)
            print res_x,x,res_y,y,cnt[0][0],tmpoffset,tmpk
    cur.execute('commit')
    print "Grid Sample: k="+str(k)
def FindFirstIndexofKeyword(keyword):
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sql="select text from coordtweets where "+box+"@>coordinate"
            cur.execute(sql)
            texts=cur.fetchall()
            i=0
            found=False
            for text in texts:
                i+=1
                sql="select to_tsvector('english','"+text[0].replace("'"," ")+"')@@to_tsquery('english','"+keyword+"')"
                cur.execute(sql)
                result=cur.fetchall()
                if result[0][0]:
                    found=True
                    break
            if found:
                cur.execute("insert into firstindex values('"+keyword+"',"+str(x)+","+str(y)+","+str(i)+","+str(len(texts))+")")
            else:
                cur.execute("insert into firstindex values('"+keyword+"',"+str(x)+","+str(y)+","+str(0)+","+str(len(texts))+")")
            cur.execute("commit")

def coarseSample(s,e):
    # keywords=GetKeywords('vectorcount',s,e)
    for x in range(0,res_x):
        for y in range(0,res_y):
            bottomleftX=x0+xStep*x
            bottomleftY=y0+yStep*y
            toprightX=x0+xStep*(x+1)
            toprightY=y0+yStep*(y+1)
            box="box '("+str(bottomleftX)+","+str(bottomleftY)+"),("+str(toprightX)+","+str(toprightY)+")'"
            sql="select count(*) from coordtweets  where "+box+"@>coordinate"
            cur.execute(sql)
            count=cur.fetchall()
            if count[0][0]==0:
                continue
            k=min(int(count[0][0]*0.15),54000)
            k=max(k,27*25)
            sql="insert into coarsesample select * from coordtweets where "+box+"@>coordinate limit "+str(k)
            cur.execute(sql)
            print res_x,x,res_y,y
    cur.execute('commit')
def createGridSample(k,offset=0,alpha=0):
    cur.execute("delete from gridsample")
    cur.execute("commit")
    time1=time.time()
    gridSampleTopCells(k,offset,alpha)
    time2=time.time()
    print time2-time1
def timeofK(keyword):
    for k1 in [0.4,0.5,0.6,0.7,0.8,0.9]:
        k=k1*3000000/100
        t=time.time()
        cur.execute("select coordinate[0],coordinate[1] from coordtweets where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+ str(k))
        cur.fetchall()
        print 'k1',k1,time.time()-t
    for k2 in range(1,99):
        k=k2*300000/100
        t=time.time()
        cur.execute("select coordinate[0],coordinate[1] from gridsample where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+ str(k))
        cur.fetchall()
        print 'k2',k2,time.time()-t
def timeofkeyword(tab,keyword,k):
    cur.execute("select * from tweets where to_tsvector('english',text)@@to_tsquery('english','job') limit 3000000")
    t=time.time()
    cur.execute("select coordinate from "+tab+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"') limit "+str(k))
    print tab,keyword,k,time.time()-t

# createGridSample(70,50,10)#random dataset
createGridSample(55,50,10)