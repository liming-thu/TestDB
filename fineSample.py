import psycopg2
import time

granularity=4

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
def myPerceptualHash(s,e):
    start=int(s)
    end=int(e)
    conn=psycopg2.connect(conStr)
    cur=conn.cursor()
    keywords=GetKeywords('vectorcount',start,end)
    if len(keywords) > 0:
        for keyword in keywords:
            coord = GetCoordinate('coord_sorted_tweets', keyword[0], -1)  # create perfect image of the file
            if len(coord)<=0:#some stop words may have no returns
                continue
            t1=time.time()
            perfectImage=imageHashFromCoordinates(coord)
            perfectLen=len(perfectImage)
            curK=0
            prevK=0
            deltaImage={}
            prevImage={}
            for i in range(70,100):
                curK=i*keyword[1]/100
                deltaCoord=coord[prevK:curK]
                deltaImage=deltaImageHashFromCoordinates(deltaCoord,prevImage)
                similarity=float(len(prevImage)+len(deltaImage))/perfectLen
                for key in deltaImage.keys():
                    prevImage[key]=1
                prevK=curK
                sql = "insert into keyword_r_q values('" + keyword[0] + "'," + str(i) + "," + str(
                    similarity) + ",'70-100')"
                cur.execute(sql)
                cur.execute("commit")
                print keyword[0],i,similarity
            t2=time.time()
            print keyword[0],t2-t1
fineSample(80000,4000000)