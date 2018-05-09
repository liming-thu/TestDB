import requests
import json
url='http://ipubmed2.ics.uci.edu:19002/query/service'
headers = {"Content-Type": "application/x-www-form-urlencoded"}
payload="select t.text from twitter.ds_tweet t;"
r=requests.post(url,data=payload,headers=headers)
for ln in r.iter_lines():
    print ln
# resArray=json.loads(r.text)["results"]

    # print resArray['elapsedTime'][0:8]
        # print i," len:",len(resArray)
        # if len(resArray)>0:
        #     fo=open("us/"+str(i)+".json",'w')
        #     fo.write(str(resArray))
        #     # for coord in resArray:
        #     #     fo.write(str(coord[0])+","+str(coord[1])+'\r')
        #     print i, "written"
        #     fo.close()
