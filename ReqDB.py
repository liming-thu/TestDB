import requests
import json

url='http://localhost:19002/query/service'
headers = {"Content-Type": "application/x-www-form-urlencoded"}
#payload="select VALUE coordinate from twitter.ds_tweet t where spatial_intersect(t.coordinate, create_rectangle(create_point(-109.060204,37.027935), create_point(-102.052130,41.003792))) and create_at>datetime(\"2017-08-08T00:00:00.000\") and create_at<datetime(\"2017-08-09T00:00:00.000\");"
for i in range (1,2):
    # payload="select VALUE coordinate from twitter.ds_tweet t where t.geo_tag.stateID="+str(i)+" and t.create_at>datetime(\"2017-08-08T00:00:00.000\") and t.create_at<datetime(\"2017-08-09T00:00:00.000\");"
    payload="select VALUE coordinate from twitter.ds_tweet t where t.create_at>datetime(\"2017-08-08T00:00:00.000\") and t.create_at<datetime(\"2017-08-09T00:00:00.000\");"
    r=requests.post(url,data=payload,headers=headers)
    resArray=json.loads(r.text)["results"]
    print i," len:",len(resArray)
    if len(resArray)>0:
        fo=open("us/"+str(i)+".dat",'w')
        fo.write(str(resArray))
        print i, "written"
        fo.close()
