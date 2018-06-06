import psycopg2
conn=psycopg2.connect("dbname='postgres' user='postgres' host='169.234.12.225' password='liming' ")
cur=conn.cursor()
# cur.execute("select id from tweets")
# cur.execute("select id,ts_rank_cd(to_tsvector('english',text),to_tsquery('english',(select string_agg(word,'|') from popwords))) from tweets limit 10")
# rows=cur.fetchall()
# i=1
# for r in rows:
#     # cur.execute(" select ts_rank_cd(to_tsvector('english',(select text from tweets where id="+str(r[0])+")),to_tsquery('english',(select string_agg(word,'|') from popwords)))")
#     # rank=cur.fetchall()[0][0]
#     cur.execute("update tweets set txteval="+str(r[0])+" where id ="+str(r[1])+"")
#     i+=1
#     print i
# file=open('10M.csv','aw+')
# cur.execute("select id from coordtweets")
# ids=cur.fetchall()
i=0
f=open("10M_text.csv")
w=open("vectors.csv",'aw+')
for ln in f.readlines():
    ln=ln.replace("'"," ").replace('uFFFD',' ')
    cur.execute("select tsvector_to_array(to_tsvector('english','"+ln+"'))")
    rows=cur.fetchall()
    for r in rows[0][0]:
        if len(r)<3:
            continue
        if any(char.isdigit() or char=='.' or char=='/' for char in r):
            continue
        w.writelines(r+"\n")
    i+=1
    if i%100000==0:
        print i
print i