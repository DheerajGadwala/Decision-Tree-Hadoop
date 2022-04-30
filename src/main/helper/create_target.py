import pandas as pd
import numpy as np

df1 = pd.read_csv("/Users/shreyasingh/CS6240/sampleddataset.csv")

def categorise(row):  
    if row['song_hotttnesss'] >= 0.0 and row['song_hotttnesss'] <= 0.45:
        return 'not_hot'

    return 'hot'

df1['Target'] = df1.apply(lambda row: categorise(row), axis=1)
df1 = df1.replace('"b','', regex=True)
df1 = df1.replace('\"','', regex=True)
df1 = df1.replace('\'','', regex=True)
df1.drop(['Unnamed: 0', 'level_0', 'index', 'Unnamed: 0.1', 'artist_mbid', 'artist_playmeid', 'artist_7digitalid', 'artist_latitude', 'artist_longitude','artist_location','song_id', 'similar_artists', 'artist_terms', 'artist_terms_freq', 'artist_terms_weight', 'analysis_sample_rate', 'audio_md5','segments_start', 'segments_confidence', 'segments_pitches', 'segments_timbre', 'segments_loudness_max', 'segments_loudness_max_time', 'segments_loudness_start', 'sections_start', 'sections_confidence', 'beats_start', 'beats_confidence', 'bars_start', 'bars_confidence', 'tatums_start', 'tatums_confidence', 'artist_mbtags', 'artist_mbtags_count'],axis = 1,inplace=True)
print(list(df1.columns))
df1.to_csv("/Users/shreyasingh/CS6240/datawithtarget.csv")