import operator
from spleeter.separator import Separator
from spleeter.audio.adapter import AudioAdapter
import pickle
from vtsm_backend.database_handler import (
    db_create_connection,
    db_create_episodes_tables, db_create_soundtrack_tables)
from vtsm_backend.vtsm_backend import (
    make_fingerprints_from_chunks,
    make_fingerprints_from_soundtracks,
    extract_soundtrack_from_files,
    separate_voices_from_music,
    make_chunks_from_episodes,
    create_chunk_matchings_database)
import argparse
import sys


def create_fingerprints_database_episodes(
        folder_name, db_name):

    db_conn = db_create_connection(db_name)
    db_create_episodes_tables(db_conn)

    extract_soundtrack_from_files(
        folder_name,
        4)
    separate_voices_from_music(
        folder_name + "_soundtrack",
        4)
    make_chunks_from_episodes(
        folder_name + "_soundtrack_separated",
        4,
        10000)
    make_fingerprints_from_chunks(
        db_conn,
        folder_name + "_chunks",
        8,
        256)

    db_conn.close()


def create_fingerprints_database_soundtrack(
        folder_name, db_name):

    db_soundtrack = db_create_connection(db_name)
    db_create_soundtrack_tables(db_soundtrack)

    make_fingerprints_from_soundtracks(
        db_soundtrack,
        folder_name,
        4,
        4)

    db_soundtrack.close()


def get_chunks_by_soundtrack(
        db_matchings, sound_id):

    db = pickle.load(open(db_matchings, "rb"))
    chunks = [(e, c) for e, c, i in db if i == sound_id]
    return chunks


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="vtsm-para: Match audio to soundtrack by parapluie48!",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-cfde',
        '--fingerprint_episodes',
        nargs=2,
        help='Create the fingerpint database of the videos\n'
             'Usages: \n'
             '-cfde path/to/episode/directory path/to/db_file')
    parser.add_argument(
        '-cfds',
        '--fingerprint_soundtrack',
        nargs=2,
        help='Create the fingerpint database of the soundtrack\n'
             'Usages: \n'
             '-cfds path/to/soundtrack/directory path/to/db_file')
    parser.add_argument(
        '-ccmd',
        '--chunks_matching_db',
        nargs=3,
        help='Create the matching database\n'
             'Usages: \n'
             '-ccmd path/to/episode_db_file'
             ' path/to/soundtrack_db_file path/to/matching_db_file')
    parser.add_argument(
        '-cbs',
        '--chunks_by_soundtrack',
        nargs=2,
        help='Get chunks by soundtrack ID\n'
             'Usages: \n'
             '-cbs path/to/matching_db_file soundtrack_id')

    args = parser.parse_args()

    if (
            not args.fingerprint_episodes
            and not args.fingerprint_soundtrack
            and not args.chunks_matching_db
            and not args.chunks_by_soundtrack):
        parser.print_help()
        sys.exit(0)

    if args.fingerprint_episodes:
        folder_n = args.fingerprint_episodes[0]
        db_n = args.fingerprint_episodes[1]

        create_fingerprints_database_episodes(
            folder_n,
            db_n)
    elif args.fingerprint_soundtrack:
        folder_n = args.fingerprint_soundtrack[0]
        db_n = args.fingerprint_soundtrack[1]
 
        create_fingerprints_database_soundtrack(
            folder_n,
            db_n)
    elif args.chunks_matching_db:
        db_episodes = args.chunks_matching_db[0]
        db_soundtrack = args.chunks_matching_db[1]
        db_matchings = args.chunks_matching_db[2]
       
        create_chunk_matchings_database(
            db_episodes,
            db_soundtrack,
            db_matchings)

    elif args.chunks_by_soundtrack:
        db_matchings = args.chunks_by_soundtrack[0]
        soundtrack_id = args.chunks_by_soundtrack[1]

        chunks = get_chunks_by_soundtrack(
            db_matchings,
            int(soundtrack_id))
        
        print(chunks)