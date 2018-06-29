import psycopg2
import time
import math

granularity=100

res_x=1920/granularity
res_y=1080/granularity

y0=15.0
y1=70.0
yStep=(y1-y0)/res_y

x0=-170.0
x1=-60.0
xStep=(x1-x0)/res_x

conStr="dbname='postgres' user='postgres' host='169.234.57.197' password='liming' "
conn=psycopg2.connect(conStr)
cur=conn.cursor()

def GetCoordinate(tb,keyword,limit):
    sql="select coordinate[0],coordinate[1] from "+tb+" where to_tsvector('english',text)@@to_tsquery('english','"+keyword+"')"
    if limit>0:
        sql+=" limit "+str(limit)
    cur.execute(sql)
    return cur.fetchall()
def GetKeywords(tb,lower,upper):
    sql="select vector from "+tb+" where count>="+str(lower)+" and count<"+str(upper)#+" and vector not in (select distinct keyword from keyword_k_q) order by count"
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
def imageHashFromCoordinates(coord):
    pixel_x=(x1-x0)/res_x
    pixel_y=(y1-y0)/res_y
    image={}
    for c in coord:
        x=float(c[0])
        y=float(c[1])
        if y<y0 or y>y1 or x>x1 or x<x0:
            continue
        else:
            index_x=int(math.floor((x-x0)/pixel_x))
            index_y=int(math.floor((y-y0)/pixel_y))
            key=str(index_x)+":"+str(index_y)
            if key not in image.keys():
                image[key]=1
    return image
def quality(s,e):
    start=int(s)
    end=int(e)
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    keywords=GetKeywords('vectorcount',start,end)
    if len(keywords) > 0:
        for keyword in keywords:
            coord = GetCoordinate('coordtweets', keyword[0], -1)  # perfect image
            if len(coord)<=0:#some stop words may have no returns
                continue
            t1=time.time()
            perfectImage=imageHashFromCoordinates(coord)
            perfectLen=len(perfectImage)
            coord2=GetCoordinate('coarsesample',keyword[0],-1)#sample image
            sampleImage=imageHashFromCoordinates(coord2)
            sampleLen=len(sampleImage)
            t2=time.time()
            print keyword[0],float(sampleLen)/float(perfectLen),t2-t1
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
# coarseSample(80000,4000000)
quality(400000,4000000)