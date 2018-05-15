import psycopg2
conn=psycopg2.connect("dbname='postgres' user='postgres' host='169.234.43.84' password='liming' ")
cur=conn.cursor()
cur.execute("select id from tweets where id<823967049949858625")
rows=cur.fetchall()
for r in rows:
    cur.execute(" select ts_rank_cd(to_tsvector('english',(select text from tweets where id="+str(r[0])+")),to_tsquery('english',(select string_agg(word,'|') from popwords)))")
    rank=cur.fetchall()[0][0]
    cur.execute("update tweets set score="+str(rank)+" where id ="+r[0]+"");