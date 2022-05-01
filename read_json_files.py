#read data from json files
import json
l1=[]
#number of articles to read
n=500000
for i in range(n):
  f = open('/dataset/'+str(i)+'.json')
  data1 = json.load(f)
  l1.append(data1)