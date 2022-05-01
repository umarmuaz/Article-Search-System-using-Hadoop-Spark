# Article-Search-System-using-Hadoop-Spark
Article Search System with implementation of TF-IDF and Vector Space Model in python using Hadoop and Spark


## Run the files in the following sequence

#### pip install -r requirements.txt
#### python load_dataset.py
#### python simple_python_tfidf.py
#### python spark_tfidf.py

#### python3 hadoop_tfidf.py --hadoop-streaming-jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -r hadoop hdfs:///dis_materials/dis_dataset/*.json --output-dir hdfs:///dis_materilas/dis_proj/output3 --no-output

for running without hadoop installation
#### python hadoop_tfidf.py -r inline *.json > output.csv
