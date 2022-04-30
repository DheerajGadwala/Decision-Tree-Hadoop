import pandas as pd

df1 = pd.read_csv("/Users/shreyasingh/CS6240/normalized.csv")

df1["track_id"] = "track_id:"+ df1["track_id"].astype(str)
df1["artist_familiarity"] = "1:"+ df1["artist_familiarity"].astype(str)
df1["artist_hotttness"] = "2:"+ df1["artist_hotttness"].astype(str)

df1["song_hotttnesss"] = "3:"+ df1["song_hotttnesss"].astype(str)
df1["danceability"] = "4:"+ df1["danceability"].astype(str)

df1["duration"] = "5:"+ df1["duration"].astype(str)
df1["energy"] = "6:"+ df1["energy"].astype(str)

df1["key"] = "7:"+ df1["key"].astype(str)
df1["key_confidence"] = "8:"+ df1["key_confidence"].astype(str)

df1["loudness"] = "9:"+ df1["loudness"].astype(str)
df1["mode"] = "10:"+ df1["mode"].astype(str)

df1["key"] = "11:"+ df1["key"].astype(str)
df1["key_confidence"] = "12:"+ df1["key_confidence"].astype(str)

df1["loudness"] = "13:"+ df1["loudness"].astype(str)
df1["mode"] = "14:"+ df1["mode"].astype(str)

df1["key"] = "15:"+ df1["key"].astype(str)
df1["key_confidence"] = "16:"+ df1["key_confidence"].astype(str)

df1["loudness"] = "17:"+ df1["loudness"].astype(str)
df1["mode"] = "18:"+ df1["mode"].astype(str)

df1["mode_confidence"] = "19:"+ df1["mode_confidence"].astype(str)
df1["start_of_fade_out"] = "20:"+ df1["start_of_fade_out"].astype(str)

df1["tempo"] = "21:"+ df1["tempo"].astype(str)
df1["time_signature"] = "22:"+ df1["time_signature"].astype(str)

df1["time_signature_confidence"] = "23:"+ df1["time_signature_confidence"].astype(str)

df1.to_csv("/Users/shreyasingh/CS6240/final.csv",index=False,header=None,sep=' ')
