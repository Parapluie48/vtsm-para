import chunk
from os import listdir
import os
from os.path import isfile, join, exists
from pydub import AudioSegment
from pydub.utils import make_chunks
import moviepy.editor as mp
from collections import Counter
import operator
import pickle
from vtsm_backend.multiproc import (
    thread_function,
    thread_pool_function)
from vtsm_backend.dejavu_m import get_file_fingerprints
from vtsm_backend.database_handler import (
    db_create_connection,
    get_distinct_episodes_chunks_from_db,
    get_fingerprints_from_episodes_chunks,
    get_matching_fingerprints_from_fingerprints,
    add_fingerprints_to_db,
    add_fingerprints_to_soundtrack_db)


def extract_audio_from_file(
        folder_name, file_name):
    if not exists(
            folder_name
            + "_audio/"
            + file_name[:-4]
            + ".mp3"):       
        clip = mp.VideoFileClip(folder_name+"/"+file_name)
        clip.audio.write_audiofile(
            folder_name
            + "_audio/"
            + file_name[:-4]+".mp3")
    else:
        print(
            folder_name
            + "_audio/"
            + file_name[:-4]
            + ".mp3", " already exists, skipping the file !")


def extract_audio_from_files(
        folder_name, nb_threads=1):
    onlyfiles = [
        f for f in listdir(folder_name)
        if isfile(join(folder_name, f))]
    try:
        os.mkdir(folder_name+"_audio")
    except Exception as e:
        print("Audio Directory already Exists!")
    thread_function(
        extract_audio_from_file,
        [(folder_name, f) for f in onlyfiles],
        nb_threads)


def file_is_spleeted(
        folder_name):
    answer = (
        os.path.exists(folder_name[:-4]+"/vocals.mp3")
        and os.path.exists(folder_name[:-4]+"/accompaniment.mp3"))
    return answer


def separate_voices_from_music(
        folder_name, nb_threads=1):
    onlyfiles = [
        f for f in listdir(folder_name)
        if (isfile(join(folder_name, f))
            and not file_is_spleeted(folder_name + "_separated/"+f))]
    print("Found file(s) to spleet: ", len(onlyfiles))
    thread_function(
        os.system,
        [("spleeter separate -p spleeter:2stems -c mp3 -o "
          + folder_name + "_separated " + "\""
          + folder_name + "/" + f + "\"",) for f in onlyfiles],
        nb_threads)


def make_chunks_from_episode(
        folder_name, file_name_num,
        chunk_size, counting_size):

    file_name = file_name_num[1]
    file_num = file_name_num[0]

    audio = AudioSegment.from_mp3(
        folder_name
        + "/"
        + file_name
        + "/accompaniment.mp3")
    chunks = make_chunks(audio, chunk_size)
    folder_string = (
        "episodes_chunks/episode_"
        + str(file_num).zfill(counting_size)
        + "_chunks")
    if not exists(folder_string):
        os.mkdir(folder_string)

    for i, ch in enumerate(chunks):
        file_string = (
            "episodes_chunks/episode_"
            + str(file_num).zfill(counting_size)
            + "_chunks/"+"chunk_"+str(i).zfill(counting_size)
            + ".mp3")
        if not exists(file_string):
            ch.export(
                file_string,
                format="mp3")
    print("Chunks made for file: ", file_name)


def make_chunks_from_episodes(
        folder_name, nb_threads=1, chunk_size=10000):
    onlyfolders = [
        f for f in listdir(folder_name) if not isfile(join(folder_name, f))]
    onlyfolders = [(i, f) for i, f in enumerate(onlyfolders)]
    counting_size = len(str(len(onlyfolders)))
    try:
        os.mkdir("episodes_chunks")
    except Exception as e:
        print("Directory already Exists!")
    thread_function(
        make_chunks_from_episode,
        [(folder_name, p, chunk_size, counting_size) for p in onlyfolders],
        nb_threads)


def make_fingerprints_from_chunk(
        episode_nb, chunk_nb,
        folder_name, file_name,
        chunk_name):
    fingerprints, _ = get_file_fingerprints(
        folder_name + "/" + file_name + "/" + chunk_name,
        None)
    return fingerprints


def get_chunks_folders(folder_name):
    onlyfolders = [
        f for f in listdir(folder_name)
        if not isfile(join(folder_name, f))]
    return onlyfolders


def get_all_chunks_from_chunk_folders(
        folder_name, chunk_folders):
    all_chunks = []
    for i, f in enumerate(chunk_folders):
        chunks = sorted([
                c for c in listdir(folder_name+"/"+f)
                if isfile(join(folder_name+"/"+f, c))])
        onlychunks = [
            (i, j, folder_name, f, c)
            for j, c in enumerate(chunks)]

        all_chunks.extend(onlychunks)
    return all_chunks


def print_chunks_fingerprinting_perc(
        total_length, previous_perc, i):
    if int((100*i)/total_length) != previous_perc:
        print(
            "Chunks fingerprinted: ",
            str(int((100*i)/total_length)),
            "%")
        previous_perc = int((100*i)/total_length)
    return previous_perc


def make_fingerprints_from_chunks(
        db, folder_name,
        nb_threads=1, db_chunk_processing=1):
    chunk_folders = get_chunks_folders(folder_name)
    all_chunks = get_all_chunks_from_chunk_folders(folder_name, chunk_folders)

    previous_perc = -1
    chunks_groups = [
        all_chunks[x:x+db_chunk_processing]
        for x in range(0, len(all_chunks), db_chunk_processing)]
    for j, processing_chunks in enumerate(chunks_groups):
        fingerprints = thread_pool_function(
            make_fingerprints_from_chunk,
            processing_chunks,
            nb_threads)

        add_fingerprints_to_db(
            db,
            fingerprints)
        previous_perc = print_chunks_fingerprinting_perc(
            len(chunks_groups),
            previous_perc,
            j)


def make_fingerprints_from_soundtrack(
        sound_id, folder_name, sound_name):
    fingerprints, _ = get_file_fingerprints(
        folder_name + "/" + sound_name,
        None)
    return fingerprints


def print_soundtracks_fingerprinting_perc(
        total_length, previous_perc, i):
    if int((100*i)/total_length) != previous_perc:
        print(
            "Soundtracks fingerprinted: ",
            str(int((100*i)/total_length)),
            "%")
        previous_perc = int((100*i)/total_length)
    return previous_perc


def make_fingerprints_from_soundtracks(
        db, folder_name,
        nb_threads=1, db_sound_processing=1):
    onlyfiles = [
        f for f in listdir(folder_name)
        if isfile(join(folder_name, f))]
    onlyfiles = [(i, f) for i, f in enumerate(onlyfiles)]

    previous_perc = -1
    sounds_chunks = [
        onlyfiles[r:r+db_sound_processing]
        for r in range(0, len(onlyfiles), db_sound_processing)]

    for j, processing_sounds in enumerate(sounds_chunks):
        fingerprints = thread_function(
            make_fingerprints_from_soundtrack,
            processing_sounds,
            nb_threads
        )
        add_fingerprints_to_soundtrack_db(
            db,
            fingerprints
        )
        previous_perc = print_soundtracks_fingerprinting_perc(
            len(sounds_chunks),
            previous_perc,
            j
        )


def compute_matching_sound_from_offsets(sounddict):
    max_occurences, max_sound_id = (-1, -1)
    for sound_id, occurences in sounddict.items():
        occurences_dict = dict(Counter(occurences))
        m_offset_diff, _ = max(
            occurences_dict.items(),
            key=operator.itemgetter(1))

        error_tolerant_counter = 0
        for offset_diff, occurence_val in occurences_dict.items():
            if (
                    m_offset_diff - 3 <= offset_diff
                    and offset_diff <= m_offset_diff+3):
                error_tolerant_counter += occurence_val

        if max_occurences < error_tolerant_counter:
            max_occurences = error_tolerant_counter
            max_sound_id = sound_id
    if max_occurences >= 10:
        return max_sound_id
    else:
        return -1
    

def offsets_dict_by_hash(matching_fingerprints):
    # We create a dictionary with hashes as key and the corresponding offset
    # and soundtrack id as values
    hashdict = {}
    for (hashh, offset, sound_id) in matching_fingerprints:
        if hashh in hashdict:
            hashdict[hashh].append((offset, sound_id))
        else:
            hashdict[hashh] = [(offset, sound_id)]
    return hashdict


def offset_differences_by_sound(chunk_fingerprints, hashdict):
    # Now, we create a dictionary with offset differences for every hash
    # and every soundtrack that matched, where the key is the soundtrack id
    # and values are the offset differences
    sounddict = {}
    for hashh, offset in chunk_fingerprints:
        if hashh in hashdict:
            for sound_offset, sound_id in hashdict[hashh]:
                if sound_id in sounddict:
                    sounddict[sound_id].append(sound_offset-offset)
                else:
                    sounddict[sound_id] = [sound_offset-offset]
    return sounddict


def get_best_matching_sound_to_chunk(
        chunk_fingerprints, matching_fingerprints):
    hash_offsets_dict = offsets_dict_by_hash(matching_fingerprints)
    sound_offsets_diff_dict = offset_differences_by_sound(
        chunk_fingerprints,
        hash_offsets_dict)
    # Now, we compute which sound had the most matches with the same offset
    # differences (we allow some error)
    matching_sound = compute_matching_sound_from_offsets(
        sound_offsets_diff_dict)
    return matching_sound


def print_chunk_matching_perc(
        total_length, previous_perc, i):
    if int((100*i)/total_length) != previous_perc:
        print(
            "Soundtracks fingerprinted: ",
            str(int((100*i)/total_length)),
            "%")
        previous_perc = int((100*i)/total_length)
    return previous_perc


def create_chunk_matchings_database(
        db_episodes, db_sound, pickle_file):
    # LAST PART: MATCH EACH CHUNK FROM EACH EPISODE TO A SOUNDTRACK
    db_conn = db_create_connection(db_episodes)
    db_soundtrack = db_create_connection(db_sound)

    distinct_episodes_chunks = get_distinct_episodes_chunks_from_db(db_conn)

    simple_db = []
    total_length = len(distinct_episodes_chunks)
    previous_perc = -1

    for i, (e, c) in enumerate(distinct_episodes_chunks):
        chunk_fingerprints = get_fingerprints_from_episodes_chunks(
            db_conn,
            e,
            c)
        matching_fingerprints = get_matching_fingerprints_from_fingerprints(
            db_soundtrack,
            [h for h, o in chunk_fingerprints])

        sound_matching = get_best_matching_sound_to_chunk(
            chunk_fingerprints,
            matching_fingerprints)

        simple_db.append((e, c, sound_matching))

        print_chunk_matching_perc(
            total_length,
            previous_perc,
            i)

    pickle.dump(simple_db, open(pickle_file, "wb"))