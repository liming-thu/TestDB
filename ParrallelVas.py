import VAS
import os
import threadpool as tp
import datetime as dt
import thread

def doVasParrallel():
    files=os.listdir("state")
    pool=tp.ThreadPool(6)
    requests=tp.makeRequests(VAS.doVasFile,files)
    [pool.putRequest(req) for req in requests]
    pool.wait()
    return

def doVasSerial():
    files=os.listdir("state")
    for f in files:
        VAS.doVasFile(f)
    return

def doVasMultiThread():
    for file in os.listdir("state"):
        thread.start_new_thread(VAS.doVasFile,(file,))
    return

startTime=dt.datetime.now()
doVasSerial()
endTime=dt.datetime.now()
print "VAS sampling time:",(endTime-startTime).seconds