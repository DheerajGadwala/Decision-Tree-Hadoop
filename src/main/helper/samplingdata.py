import pandas as pd
import numpy as np

df1 = pd.read_csv("/Users/shreyasingh/CS6240/fulldataset.csv")

df = pd.read_csv("/Users/shreyasingh/CS6240/fulldataset.csv")
df.drop(df.index[df['song_hotttnesss'] == 0.0], inplace=True)
index_with_nan = df.index[df.isnull().any(axis=1)]
df.drop(index_with_nan,0, inplace=True)
df.dropna(subset=['song_hotttnesss'])
df = df.reset_index()
df.to_csv("/Users/shreyasingh/CS6240/datasetwithsonghotness.csv")

df2 = df1.loc[(df1['song_hotttnesss'] == 0.0)]
df3 = df2.sample(50)

frames = [df, df3]

result = pd.concat(frames)
result = result.reset_index()
result.to_csv("/Users/shreyasingh/CS6240/sampleddataset.csv")