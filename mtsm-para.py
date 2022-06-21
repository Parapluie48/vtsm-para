import operator
from spleeter.separator import Separator
from spleeter.audio.adapter import AudioAdapter
import pickle
from mtsm_backend.database_handler import (db_create_connection,db_create_episodes_tables,db_create_soundtrack_tables)
from mtsm_backend.mtsm_backend import (make_fingerprints_from_chunks,make_fingerprints_from_soundtracks,extract_soundtrack_from_files,separate_voices_from_music,make_chunks_from_episodes,create_chunk_matchings_database)
import argparse
import sys



def create_fingerprints_database_episodes(folder_name,db_name):
    db_conn=db_create_connection(db_name)
    db_create_episodes_tables(db_conn)

    extract_soundtrack_from_files(folder_name,4)
    separate_voices_from_music(folder_name+"_soundtrack",4)
    make_chunks_from_episodes(folder_name+"_soundtrack_separated",4,10000)
    make_fingerprints_from_chunks(db_conn,folder_name+"_chunks",8,256)

    db_conn.close()

def create_fingerprints_database_soundtrack(folder_name,db_name):

    db_soundtrack=db_create_connection(db_name)
    db_create_soundtrack_tables(db_soundtrack)

    make_fingerprints_from_soundtracks(db_soundtrack,folder_name,4,4)

    db_soundtrack.close()


def get_chunks_by_soundtrack(db_matchings,sound_id):
    db=pickle.load(open(db_matchings,"rb"))
    chunks=[(e,c) for e,c,i in db if i==sound_id]
    return chunks

if __name__ == '__main__':
    parser=argparse.ArgumentParser(
        description="mtsm-para: Match audio to soundtrack !",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-cfde','--fingerprint_episodes',nargs=2,
                        help='Create the fingerpint database of the videos')
    parser.add_argument('-cfds','--fingerprint_soundtrack',nargs=2,
                        help='Create the fingerpint database of the soundtrack')
    parser.add_argument('-ccmd','--chunk_matching_db',nargs=3,
                        help='Create the matching database')
    parser.add_argument('-cbs','--chunks_by_soundtrack',nargs=2,
                        help='Get chunks by soundtrack ID')

    args=parser.parse_args()

    if not args.fingerprint_episodes and not args.fingerprint_soundtrack and not args.chunk_matching_db and not args.chunks_by_soundtrack:
        parser.print_help()
        sys.exit(0)
    
    if args.fingerprint_episodes:
        folder_n=args.fingerprint_episodes[0]
        db_n=args.fingerprint_episodes[1]
        
        create_fingerprints_database_episodes(folder_n,db_n)
    elif args.fingerprint_soundtrack:
        print("soundtrack")
    elif args.chunks_matching_db:
        print("matching db")
    elif args.chunks_by_soundtrack:
        print("chunks by soundtrack")

"""
folder_name="episodes"
folder_name_soundtrack="soundtrack"
db_episodes="episodes_db.db"
db_soundtrack="soundtrack_db.db"
db_matchings="simple_db.pickle"


create_fingerprints_database_episodes(folder_name,db_episodes)

create_fingerprints_database_soundtrack(folder_name_soundtrack,db_soundtrack)

create_chunk_matchings_database(db_episodes,db_soundtrack,db_matchings)

chunks=get_chunks_by_soundtrack(db_matchings,97)
print(chunks)
"""