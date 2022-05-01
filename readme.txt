pip install -r requirements.txt
python load_dataset.py
python simple_python_tfidf.py
python spark_tfidf.py
python hadoop_tfidf.py -r inline *.json > output.csv