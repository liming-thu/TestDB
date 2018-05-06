import requests
import json
url='http://localhost:19002/query/service'
headers = {"Content-Type": "application/x-www-form-urlencoded"}
payload="select t.text from twitter.ds_tweet t where id=854093020022624256;"
r=requests.post(url,data=payload,headers=headers)
# resArray=json.loads(r.text)["results"]
# f = open("db.dat",'w')
# f.write(str(resArray))
# f.close()
# payload="select VALUE coordinate from twitter.ds_tweet t where spatial_intersect(t.coordinate, create_rectangle(create_point(-109.060204,37.027935), create_point(-102.052130,41.003792))) and create_at>datetime(\"2017-08-08T00:00:00.000\") and create_at<datetime(\"2017-08-09T00:00:00.000\");"
# for str in ['liming',
#             'China',
#             'seven',
#             'stone',
#             'major',
#             'games',
#             'race',
#             'peace',
#             'table',
#             'shirt',
#             'action',
#             'train',
#             'success',
#             'strong',
#             'trump',
#             'lunch',
#             'university',
#             'manager',
#             'hospitality',
#             'apply',
#             'great',
#             'work'
#             ]:
    # for i in range (1,11):
    #     payload="select count(*) from twitter.ds_tweet t where ftcontains(t.text,['"+str+"']);"
    #     r=requests.post(url,data=payload,headers=headers)
    #     resArray=json.loads(r.text)["metrics"]
    # print str,"select count time:",resArray['elapsedTime'][-2:]
    # for i in range (1,11):
    #      payload="select count(*) from twitter.ds_tweet t where ftcontains(t.text,['"+str+"']);"
    #      r=requests.post(url,data=payload,headers=headers)
    #      resArray=json.loads(r.text)["metrics"]
    #      print resArray['elapsedTime'][0:8]
    # ##################################################
    # for i in range (1,11):
    #     payload="select text from twitter.ds_tweet t where ftcontains(t.text,['"+str+"']);"
    #     r=requests.post(url,data=payload,headers=headers)
    #     resArray=json.loads(r.text)["metrics"]
    #
    # print str,"select text time:",resArray['elapsedTime'][-2:]
    # for i in range (1,11):
    # payload="select text from twitter.ds_tweet t where ftcontains(t.text,['"+str+"']);"
    # r=requests.post(url,data=payload,headers=headers)
    # resArray=json.loads(r.text)["metrics"]
    # print resArray['elapsedTime'][0:8]
        # print i," len:",len(resArray)
        # if len(resArray)>0:
        #     fo=open("us/"+str(i)+".json",'w')
        #     fo.write(str(resArray))
        #     # for coord in resArray:
        #     #     fo.write(str(coord[0])+","+str(coord[1])+'\r')
        #     print i, "written"
        #     fo.close()
