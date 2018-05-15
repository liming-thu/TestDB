import json;
import gzip;
import re;

# file=gzip.open("/root/cloudberry/examples/twittermap/script/sample.adm.gz")
file=open("/root/sampleWithCoordinate.adm")
ln=file.readline()
fields=open("id_text_coordinate.csv",'aw+');
i=0
while ln!=None:
    try:
        id=ln[ln.index("\"id\": ")+6:ln.index(", \"text\"")];
    except:
        continue
    try:
        text=ln[ln.index("\"text\": \"")+9:ln.index("\", \"in_reply_to_status\"")]
    except:
        text="None"
    try:
        coordinate=ln[ln.index("\"coordinate\": point")+21:ln.index("\")"", \"retweet_count\"")]
    except:
        coordinate="None"

    ln=file.readline()
    if coordinate!="None":
        fields.write(id+",\""+text+"\",\"("+coordinate+")\"\n")
        i+=1
        if i%10000==0:
            print (i)
print (i)
file.close()
fields.close()