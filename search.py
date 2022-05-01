import pandas as pd
df = pd.read_csv('output.csv',delimiter=',|\t')
df.columns =['word', 'file','tfidf']
df['word']=df['word'].str.replace('"', '')
df['word']=df['word'].str.replace('[', '')
df['file']=df['file'].str.replace('"', '')
df['file']=df['file'].str.replace(']', '')
df['file']=df['file'].str.replace('file://', '')

query='airport'
res_df=df[['file','tfidf']].loc[df['word'] == query]
res_df.sort_values('tfidf',ascending=False)