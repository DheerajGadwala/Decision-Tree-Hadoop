import pandas as pd
from sklearn import preprocessing

def categorise(row):
    if row['song_hotttnesss'] >= 0.0 and row['song_hotttnesss'] <= 0.2:
        return 0
    elif row['song_hotttnesss'] > 0.2 and row['song_hotttnesss'] <= 0.4:
        return 1
    elif row['song_hotttnesss'] > 0.4 and row['song_hotttnesss'] <= 0.6:
        return 2
    elif row['song_hotttnesss'] > 0.6 and row['song_hotttnesss'] <= 0.8:
        return 3
    return 4

df1 = pd.read_csv("/Users/shreyasingh/CS6240/sampleddataset.csv",index_col=0)

df1['Target'] = df1.apply(lambda row: categorise(row), axis=1)

df1.drop(['level_0', 'index', 'Unnamed: 0.1','artist_id' ,'artist_mbid', 'artist_playmeid', 'artist_7digitalid', 'artist_latitude', 'artist_longitude','artist_location','artist_name','song_id','release','release_7digitalid', 'title','similar_artists', 'artist_terms', 'artist_terms_freq', 'artist_terms_weight', 'analysis_sample_rate', 'audio_md5','track_id','segments_start', 'segments_confidence', 'segments_pitches', 'segments_timbre', 'segments_loudness_max', 'segments_loudness_max_time', 'segments_loudness_start', 'sections_start', 'sections_confidence', 'beats_start', 'beats_confidence', 'bars_start', 'bars_confidence', 'tatums_start', 'tatums_confidence', 'artist_mbtags', 'artist_mbtags_count','year'],axis = 1,inplace=True)
print(list(df1.columns))
numbers = df1["Target"]
track = df1["track_7digitalid"]
df1.drop(['Target','track_7digitalid'],axis = 1,inplace=True)

x = df1.values #returns a numpy array
min_max_scaler = preprocessing.MinMaxScaler()
x_scaled = min_max_scaler.fit_transform(x)
df = pd.DataFrame(x_scaled,columns=df1.columns)
df = df.join(numbers)
df = df.join(track)
first_column = df.pop('Target')
sec_col = df.pop('track_7digitalid')
df.insert(0, 'Target', first_column)
df.insert(1,'track_id', sec_col)
df.to_csv("/Users/shreyasingh/CS6240/normalized.csv",index=False)