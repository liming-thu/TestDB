import psycopg2
import cv2 as cv
import matplotlib.pyplot as plt
import os
import sys

def FindingKofQ(keyword,quality=.85):
    conn=psycopg2.connect("dbname='postgres' user='postgres' host='169.234.12.225' password='liming' ")
    cur=conn.cursor()
    sql="select k,q from keyword_k_q where keyword='"+keyword+"' and q<="+str(quality)+" and d='coord_biased' order by 1 desc limit 1"
    cur.execute(sql)
    lower=cur.fetchall()
    sql="select k,q from keyword_k_q where keyword='"+keyword+"' and q>"+str(quality)+" and d='coord_biased' order by 1 limit 1"
    cur.execute(sql)
    upper=cur.fetchall()
    #finding out which is more close to the specified quality
    if len(lower)==0 or len(upper)==0:
        return False
    if quality-lower[0][1]<=upper[0][1]-quality:
        return int((1+quality-lower[0][1])*lower[0][0])
    else:
        return int((1-upper[0][1]+quality)*upper[0][0])
def GetKeywords(tb,lower,upper):
    conn=psycopg2.connect("dbname='postgres' user='postgres' host='169.234.12.225' password='liming' ")
    cur=conn.cursor()
    sql="select vector,count from "+tb+" where count>="+str(lower)+" and count<"+str(upper)+" and vector in (select distinct keyword from keyword_k_q) order by count"
    cur.execute(sql)
    return cur.fetchall()
def main():
    for keyword in GetKeywords('vectorcount',5000,4000000):
        ret=FindingKofQ(keyword[0])
        if ret!=False:
            print keyword[0],keyword[1],FindingKofQ(keyword[0]),float(FindingKofQ(keyword[0]))/keyword[1]

main()