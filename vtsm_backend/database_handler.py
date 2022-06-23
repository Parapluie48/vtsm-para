import sqlite3
from vtsm_backend.sql_queries import (
    CREATE_FINGERPRINTS_TABLE,
    CREATE_FINGERPRINTS_TABLE_SOUNDTRACK,
    CREATE_SOUNDTRACK_TABLE,
    INSERT_FINGERPRINT,
    CREATE_INDEX,
    SELECT_FINGERPRINT,
    INSERT_FINGERPRINT_SOUNDTRACK,
    INSERT_SOUNDTRACK,
    SELECT_DISTINCT_EPISODE_CHUNK,
    SELECT_HASHES_EPISODE_CHUNK,
    SELECT_FINGERPRINT_FROM_HASHES)


def db_create_connection(db_file_name):
    conn = sqlite3.connect(db_file_name)
    return conn


def db_create_soundtrack_tables(db):
    c = db.cursor()
    c.execute(CREATE_FINGERPRINTS_TABLE_SOUNDTRACK)
    c.execute(CREATE_SOUNDTRACK_TABLE)
    c.execute(CREATE_INDEX)
    db.commit()


def db_create_episodes_tables(db):
    c = db.cursor()
    c.execute(CREATE_FINGERPRINTS_TABLE)
    c.execute(CREATE_INDEX)
    db.commit()


def get_distinct_episodes_chunks_from_db(db):
    c = db.cursor()
    r = c.execute(SELECT_DISTINCT_EPISODE_CHUNK)
    return [data for data in r]


def get_fingerprints_from_episodes_chunks(
        db, episode_id, chunk_id):
    c = db.cursor()
    r = c.execute(
        SELECT_HASHES_EPISODE_CHUNK,
        (episode_id, chunk_id))
    return [
        (
            h[1:-1],
            int.from_bytes(
                o,
                byteorder="little",
                signed=False))
        for h, o in r]


def get_matching_fingerprints_from_fingerprints(
        db, fingerprint_list):
    c = db.cursor()
    r = c.execute(
        SELECT_FINGERPRINT_FROM_HASHES
        % ", ".join(["quote(?)"]*len(fingerprint_list)),
        fingerprint_list)
    return [
        (
            h[1:-1],
            int.from_bytes(
                o,
                byteorder="little",
                signed=False), i)
        for h, o, i in r]


def add_fingerprints_to_db(
        db, fingerprints):
    for (i, j, _, _, c), results in fingerprints:
        c = db.cursor()
        c.executemany(
            INSERT_FINGERPRINT,
            [(i, j, h, o) for h, o in results])
        db.commit()


def add_fingerprints_to_soundtrack_db(
        db, fingerprints):
    for (sound_id, _, file_name), results in fingerprints:
        c = db.cursor()
        c.execute(
            INSERT_SOUNDTRACK,
            [sound_id, file_name])
        c.executemany(
            INSERT_FINGERPRINT_SOUNDTRACK,
            [(sound_id, h, o) for h, o in results])
        db.commit()