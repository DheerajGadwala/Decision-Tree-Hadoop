import glob
import tables as tb
import numpy as np
import hdf5_getters
import os
import pandas as pd
import sys

def res_scraper(hdf5path):
    if not os.path.isfile(hdf5path):
        print ('ERROR: file',hdf5path,'does not exist.')
        sys.exit(0)
    h5 = hdf5_getters.open_h5_file_read(hdf5path)
    numSongs = hdf5_getters.get_num_songs(h5)

    final_data = []

    info_scraped = {}
	

    info_scraped['artist_familiarity'] = hdf5_getters.get_artist_familiarity(h5)
    info_scraped['artist_hotttness'] = hdf5_getters.get_artist_hotttnesss(h5)
    info_scraped['artist_id'] = hdf5_getters.get_artist_id(h5)
    info_scraped['artist_mbid'] = hdf5_getters.get_artist_mbid(h5)
    info_scraped['artist_playmeid'] = hdf5_getters.get_artist_playmeid(h5)
    info_scraped['artist_7digitalid'] = hdf5_getters.get_artist_7digitalid(h5)
    info_scraped['artist_latitude'] = hdf5_getters.get_artist_latitude(h5)
    info_scraped['artist_longitude'] = hdf5_getters.get_artist_longitude(h5)
    
    info_scraped['artist_location'] = hdf5_getters.get_artist_location(h5)
    info_scraped['artist_name'] = hdf5_getters.get_artist_name(h5)
    info_scraped['release'] = hdf5_getters.get_release(h5)
    info_scraped['release_7digitalid'] = hdf5_getters.get_release_7digitalid(h5)
    info_scraped['song_id'] = hdf5_getters.get_song_id(h5)
    info_scraped['song_hotttnesss'] = hdf5_getters.get_song_hotttnesss(h5)
    info_scraped['title'] = hdf5_getters.get_title(h5)
    info_scraped['track_7digitalid'] = hdf5_getters.get_track_7digitalid(h5) 

    info_scraped['similar_artists'] = hdf5_getters.get_similar_artists(h5)
    info_scraped['artist_terms'] = hdf5_getters.get_artist_terms(h5)
    info_scraped['artist_terms_freq'] = hdf5_getters.get_artist_terms_freq(h5)
    info_scraped['artist_terms_weight'] = hdf5_getters.get_artist_terms_weight(h5)
    info_scraped['analysis_sample_rate'] = hdf5_getters.get_analysis_sample_rate(h5)
    info_scraped['audio_md5'] = hdf5_getters.get_audio_md5(h5)
    info_scraped['danceability'] = hdf5_getters.get_danceability(h5)
    info_scraped['duration'] = hdf5_getters.get_duration(h5)

    info_scraped['energy'] = hdf5_getters.get_energy(h5)
    info_scraped['key'] = hdf5_getters.get_key(h5)
    info_scraped['key_confidence'] = hdf5_getters.get_key_confidence(h5)
    info_scraped['loudness'] = hdf5_getters.get_loudness(h5)
    info_scraped['mode'] = hdf5_getters.get_mode(h5)
    info_scraped['mode_confidence'] = hdf5_getters.get_mode_confidence(h5)
    info_scraped['start_of_fade_out'] = hdf5_getters.get_start_of_fade_out(h5)
    info_scraped['tempo'] = hdf5_getters.get_tempo(h5)

    info_scraped['energy'] = hdf5_getters.get_energy(h5)
    info_scraped['key'] = hdf5_getters.get_key(h5)
    info_scraped['key_confidence'] = hdf5_getters.get_key_confidence(h5)
    info_scraped['loudness'] = hdf5_getters.get_loudness(h5)
    info_scraped['mode'] = hdf5_getters.get_mode(h5)
    info_scraped['mode_confidence'] = hdf5_getters.get_mode_confidence(h5)
    info_scraped['start_of_fade_out'] = hdf5_getters.get_start_of_fade_out(h5)
    info_scraped['tempo'] = hdf5_getters.get_tempo(h5)   

    info_scraped['time_signature'] = hdf5_getters.get_time_signature(h5)
    info_scraped['time_signature_confidence'] = hdf5_getters.get_time_signature_confidence(h5)
    info_scraped['track_id'] = hdf5_getters.get_track_id(h5)
    info_scraped['segments_start'] = hdf5_getters.get_segments_start(h5)
    info_scraped['segments_confidence'] = hdf5_getters.get_segments_confidence(h5)
    info_scraped['segments_pitches'] = hdf5_getters.get_segments_pitches(h5)
    info_scraped['segments_timbre'] = hdf5_getters.get_segments_timbre(h5)
    info_scraped['segments_loudness_max'] = hdf5_getters.get_segments_loudness_max(h5)

    info_scraped['segments_loudness_max_time'] = hdf5_getters.get_segments_loudness_max_time(h5)
    info_scraped['segments_loudness_start'] = hdf5_getters.get_segments_loudness_start(h5)
    info_scraped['sections_start'] = hdf5_getters.get_sections_start(h5)
    info_scraped['sections_confidence'] = hdf5_getters.get_sections_confidence(h5)
    info_scraped['beats_start'] = hdf5_getters.get_beats_start(h5)
    info_scraped['beats_confidence'] = hdf5_getters.get_beats_confidence(h5)
    info_scraped['bars_start'] = hdf5_getters.get_bars_start(h5)
    info_scraped['bars_confidence'] = hdf5_getters.get_bars_confidence(h5)

    info_scraped['tatums_start'] = hdf5_getters.get_tatums_start(h5)
    info_scraped['tatums_confidence'] = hdf5_getters.get_tatums_confidence(h5)
    info_scraped['artist_mbtags'] = hdf5_getters.get_artist_mbtags(h5)
    info_scraped['artist_mbtags_count'] = hdf5_getters.get_artist_mbtags_count(h5)
    info_scraped['year'] = hdf5_getters.get_year(h5)
    
    h5.close()

    final_data.append(info_scraped)

    df = pd.DataFrame(final_data)
    df.index += 1
    print(df)

    return df



list_of_files =[]

def jsonTocsv(dir):
    for f_name in os.listdir(dir):
        print(f_name)
        if f_name.endswith('.h5'):
            list_of_files.append(dir+'/'+f_name)
    
    print(list_of_files)


jsonTocsv('/Users/shreyasingh/CS6240/millionsongdataset')

review_data = []

for i in range(len(list_of_files)):
    resreview = res_scraper(list_of_files[i])
    review_data.append(resreview)
   
print(review_data)   
# encoding is utf-8-sig
review_all = pd.concat(review_data)
print(review_all)
review_all.to_csv("/Users/shreyasingh/CS6240/fulldataset.csv")