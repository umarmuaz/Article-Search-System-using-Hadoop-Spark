from datasets import load_dataset
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.sql import SparkSession
import time

exec(open("read_json_files.py").read())

conf = SparkConf().setMaster("local[*]").setAppName("SparkTFIDF")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

rawData = sc.parallelize(l1)
start = time.time()

kecilRawData = rawData.map(lambda x: x.lower())
fields = kecilRawData.map(lambda x: x.split("\t"))
documents = fields.map(lambda x: x[1].split(" "))
documentId = fields.map(lambda x: x[0])

hashingTF = HashingTF(100000)
tf = hashingTF.transform(documents)

tf.cache()
idf = IDF(minDocFreq=0).fit(tf)

tfidf = idf.transform(tf)

end = time.time()

total_time = end - start
print("\n"+ str(total_time))