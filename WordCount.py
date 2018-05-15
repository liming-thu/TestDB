import nltk
from nltk.corpus import stopwords
from os import listdir
from collections import OrderedDict
import re
# nltk.download('stopwords')
# nltk.download('punkt')

stop_words=set(stopwords.words("english"))
stop_words=stop_words.union({u'http', u'https' ,u'www' ,u'_',u'.',u'/','?',';','<','>',':','"','[',']','{','}','!','~','`','@','#','$','%','^','&','*','(',')','-','+','=','\\','|'})
porter=nltk.PorterStemmer()
i=0
for f in listdir('../VasPP/split/'):
    file=open('../VasPP/split/'+f)
    wc=open('wc/'+f,'aw+')
    dict=OrderedDict()
    ln=file.readline()
    while ln:
        wl=re.findall(r'\w+',ln.lower())
        for w in wl:
            x=porter.stem(w)
            # x=w
            if x not in stop_words and not x.isdigit() and len(x)>2:
                if dict.has_key(x):
                    dict[x]+=1
                else:
                    dict.update({x:1})
        ln=file.readline()
        i+=1
        if i%10000==0:
            print f,i
    for w in dict:
       wc.write(w+" "+str(dict[w])+'\n')
    file.close()
    wc.close()