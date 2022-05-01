import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import string
import numpy as np

import time

exec(open("read_json_files.py").read())

#calculate the tfidf using tfidf vectorizor

start = time.time()
vectorizer = TfidfVectorizer(analyzer='word' , stop_words='english',min_df=3,token_pattern=r'(?u)\b[A-Za-z]+\b')
x = vectorizer.fit_transform(l1)

end = time.time()

total_time = end - start
print("\n"+ str(total_time))