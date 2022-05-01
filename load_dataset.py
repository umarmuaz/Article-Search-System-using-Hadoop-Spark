from datasets import load_dataset
import json
#Load Dataset and store in json files

data=load_dataset("wikipedia", "20220301.en")
#number of articles to read
n=500000

j=0
for i in data['train']:
    json_string = json.dumps(str(j)+'\t'+i['text'])
    with open('/dataset/'+str(j)+'.json', 'w') as f:
        json.dump(json_string, f, indent=3)
    j+=1
    if j>n:
      break