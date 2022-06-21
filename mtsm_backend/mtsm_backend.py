from os import listdir
import os
from os.path import isfile, join, exists
from pydub import AudioSegment
from pydub.utils import make_chunks
import moviepy.editor as mp
from mtsm_backend.multiproc import thread_function,thread_pool_function
from mtsm_backend.dejavu_m import get_file_fingerprints
from mtsm_backend.database_handler import add_fingerprints_to_db,add_fingerprints_to_soundtrack_db
from collections import Counter
import operator
import pickle
from mtsm_backend.database_handler import (db_create_connection,get_distinct_episodes_chunks_from_db,
                                            get_fingerprints_from_episodes_chunks,get_matching_fingerprints_from_fingerprints)

def extract_soundtrack_from_file(folder_name,file_name):
    if not exists(folder_name+"_soundtrack/"+file_name[:-3]+".mp3"):
        clip=mp.VideoFileClip(folder_name+"/"+file_name)
        clip.audio.write_audiofile(folder_name+"_soundtrack/"+file_name[:-3]+".mp3")
    else:
        print(folder_name+"_soundtrack/"+file_name[:-3]+".mp3", " already exists, skipping the file !")

def extract_soundtrack_from_files(folder_name,nb_threads=1):
    onlyfiles = [f for f in listdir(folder_name) if isfile(join(folder_name, f))]
    try:
        os.mkdir(folder_name+"_soundtrack")
    except:
        print("Directory already Exists!")
    thread_function(extract_soundtrack_from_file,[(folder_name,f) for f in onlyfiles],nb_threads)


def separate_voices_from_music(folder_name,nb_threads=1):
    onlyfiles = [f for f in listdir(folder_name) if isfile(join(folder_name, f))]
    
    thread_function(os.system,[("spleeter separate -p spleeter:2stems -c mp3 -o "+folder_name+"_separated "+"\""+folder_name+"/"+f+"\"",) for f in onlyfiles],nb_threads)

def make_chunks_from_episode(folder_name,file_name_num,chunk_size,counting_size):
    file_name=file_name_num[1]
    file_num=file_name_num[0]

    audio=AudioSegment.from_mp3(folder_name+"/"+file_name+"/accompaniment.mp3")
    chunks=make_chunks(audio,chunk_size)
    os.mkdir("episodes_chunks/episode_"+str(file_num).zfill(counting_size)+"_chunks")

    c_counting_size=len(str(len(chunks)))

    for i,ch in enumerate(chunks):
        ch.export("episodes_chunks/episode_"+str(file_num).zfill(counting_size)+"_chunks/"+"chunk_"+str(i).zfill(counting_size)+".mp3",format="mp3")
    print("Chunks made for file: ",file_name)

def make_chunks_from_episodes(folder_name,nb_threads=1,chunk_size=10000):
    onlyfolders = [f for f in listdir(folder_name) if not isfile(join(folder_name,f))]
    onlyfolders=[(i,f) for i,f in enumerate(onlyfolders)]
    counting_size=len(str(len(onlyfolders)))
    try:
        os.mkdir("episodes_chunks")
    except:
        print("Directory already Exists!")
    thread_function(make_chunks_from_episode,[(folder_name,p,chunk_size,counting_size) for p in onlyfolders],nb_threads)

def make_fingerprints_from_chunk(episode_nb,chunk_nb,folder_name,file_name,chunk_name):
    fingerprints,_ = get_file_fingerprints(folder_name+"/"+file_name+"/"+chunk_name,None)
    return fingerprints


def make_fingerprints_from_chunks(db,folder_name,nb_threads=1,db_chunk_processing=1):
    onlyfolders = [f for f in listdir(folder_name) if not isfile(join(folder_name,f))]

    all_chunks=[]
    for i,f in enumerate(onlyfolders):
        onlychunks=[(i,j,folder_name,f,c) for j,c in enumerate(sorted([c for c in listdir(folder_name+"/"+f) if isfile(join(folder_name+"/"+f,c))]))]
        all_chunks.extend(onlychunks)

    processing_chunks=[]
    total_length=len(all_chunks)
    previous_perc=-1
    for i,ch in enumerate(all_chunks):
        processing_chunks.append(ch)
        if len(processing_chunks)>=db_chunk_processing:
            fingerprints=thread_pool_function(make_fingerprints_from_chunk,processing_chunks,nb_threads)
            add_fingerprints_to_db(db,fingerprints)
            processing_chunks=[]
            if int((100*i)/total_length)!=previous_perc:
                print("Chunks fingerprinted: ",str(int((100*i)/total_length)),"%")
                previous_perc=int((100*i)/total_length)
    #Complete missing ones
    if len(processing_chunks)>0:
        fingerprints=thread_pool_function(make_fingerprints_from_chunk,processing_chunks,nb_threads)
        add_fingerprints_to_db(db,fingerprints)
        processing_chunks=[]
        if int((100*i)/total_length)!=previous_perc:
            print("Chunks fingerprinted: ",str(int((100*i)/total_length)),"%")
            previous_perc=int((100*i)/total_length)

def make_fingerprints_from_soundtrack(sound_id,folder_name,sound_name):
    fingerprints,_=get_file_fingerprints(folder_name+"/"+sound_name,None)
    return fingerprints

def make_fingerprints_from_soundtracks(db,folder_name,nb_threads=1,db_sound_processing=1):
    onlyfiles=[f for f in listdir(folder_name) if isfile(join(folder_name,f))]

    processing_sounds=[]
    total_length=len(onlyfiles)
    previous_perc=-1
    for i,s in enumerate(onlyfiles):
        processing_sounds.append((i,folder_name,s))
        if len(processing_sounds)>=db_sound_processing:
            fingerprints=thread_pool_function(make_fingerprints_from_soundtrack,processing_sounds,nb_threads)
            add_fingerprints_to_soundtrack_db(db,fingerprints)
            processing_sounds=[]
            if int((100*i)/total_length)!=previous_perc:
                print("Soundtracks fingerprinted: ",str(int((100*i)/total_length)),"%")
                previous_perc=int((100*i)/total_length)
    #Complete the missing ones
    if len(processing_sounds)>0:
        fingerprints=thread_pool_function(make_fingerprints_from_soundtrack,processing_sounds,nb_threads)
        add_fingerprints_to_soundtrack_db(db,fingerprints)
        processing_sounds=[]
        if int((100*i)/total_length)!=previous_perc:
            print("Soundtracks fingerprinted: ",str(int((100*i)/total_length)),"%")
            previous_perc=int((100*i)/total_length)

def get_best_matching_sound_to_chunk(chunk_fingerprints,matching_fingerprints):
    hashdict={}
    for (hashh,offset,sound_id) in matching_fingerprints:
        if hashh in hashdict:
            hashdict[hashh].append((offset,sound_id))
        else:
            hashdict[hashh]=[(offset,sound_id)]
    #We created a dictionary with hashes as key and the corresponding offset and soundtrack id as values
    #Now, we create a dictionary with offset differences for every hash and every soundtrack that matched, where the key is the soundtrack id and values are the offset differences
    sounddict={}
    for hashh,offset in chunk_fingerprints:
        if hashh in hashdict:
            for sound_offset, sound_id in hashdict[hashh]:
                if sound_id in sounddict:
                    sounddict[sound_id].append(sound_offset-offset)
                else:
                    sounddict[sound_id]=[sound_offset-offset]
    #Now, we compute which sound had the most matches with the same offset differences (we allow some error)                
    max_occurences=-1
    max_sound_id=-1

    for sound_id,occurences in sounddict.items():
        occurences_dict=dict(Counter(occurences))
        m_offset_diff,_=max(occurences_dict.items(), key=operator.itemgetter(1))
        
        error_tolerant_counter=0
        for offset_diff,occurence_val in occurences_dict.items():
            if m_offset_diff-3<=offset_diff and offset_diff<=m_offset_diff+3:
                error_tolerant_counter+=occurence_val

        if max_occurences<error_tolerant_counter:
            max_occurences=error_tolerant_counter
            max_sound_id=sound_id
    if max_occurences>=10:
        return max_sound_id
    else:
        return -1

def create_chunk_matchings_database(db_episodes,db_sound,pickle_file):
    #LAST PART: MATCH EACH CHUNK FROM EACH EPISODE TO A SOUNDTRACK
    db_conn=db_create_connection(db_episodes)
    db_soundtrack=db_create_connection(db_sound)

    distinct_episodes_chunks=get_distinct_episodes_chunks_from_db(db_conn)

    simple_db=[]
    total_length=len(distinct_episodes_chunks)
    previous_perc=-1

    for i,(e,c) in enumerate(distinct_episodes_chunks):
        chunk_fingerprints=get_fingerprints_from_episodes_chunks(db_conn,e,c)
        matching_fingerprints=get_matching_fingerprints_from_fingerprints(db_soundtrack,[h for h,o in chunk_fingerprints])

        sound_matching=get_best_matching_sound_to_chunk(chunk_fingerprints,matching_fingerprints)

        simple_db.append((e,c,sound_matching))
        
        if int((100*i)/total_length)!=previous_perc:
            print("Chunks matched: ",str(int((100*i)/total_length)),"%")
            previous_perc=int((100*i)/total_length)

    pickle.dump(simple_db,open(pickle_file,"wb"))