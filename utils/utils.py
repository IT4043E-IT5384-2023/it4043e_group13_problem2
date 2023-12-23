from google.cloud import storage
from pymongo import MongoClient
import json
import os
import psycopg2


# I/O
def save_json(file_path, data):
    with open(file_path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=str)


def load_json(file_path):
    if not os.path.exists(file_path):
        return {}
    with open(file_path, "r") as f:
        return json.load(f)


# Google Cloud Storage
def get_gc_bucket():
    client = storage.Client.from_service_account_json(os.getenv("GCS_CREDENTIALS_PATH"))
    bucket = client.get_bucket(os.getenv("GCS_BUCKET"))
    return bucket


def get_gc_blob(bucket, blob_name):
    blob = bucket.get_blob(blob_name)
    return blob


def upload_gc_from_file(bucket, source_file_name, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    return blob

def write_gc_json_blob(bucket, blob_name, data):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(
        data=json.dumps(data),
        content_type="application/json",
    )
    
def read_gc_json_blob(bucket, blob_name):
    blob = bucket.get_blob(blob_name)
    return json.loads(blob.download_as_string())

# MongoDB
def get_mg_client(conn_str):
    return MongoClient(conn_str)


def get_mg_collection(client, db_name, collection_name):
    db = client[db_name]
    return db[collection_name]


def get_chain_db(chain):
    chains = {
        "chain_0x1": "ethereum_blockchain_etl",
        "chain_0x38": "blockchain_etl",
        "chain_0x89": "polygon_blockchain_etl",
        "chain_0xfa": "ftm_blockchain_etl",
        "chain_0xa4b1": "arbitrum_blockchain_etl",
        "chain_0xa": "optimism_blockchain_etl",
        "chain_0xa86a": "avalanche_blockchain_etl",
    }
    if chain not in chains:
        raise Exception("Chain not supported")
    return chains[chain]


# PostgreSQL
def get_pg_conn(db, host, user, password, port):
    pg_conn = psycopg2.connect(
        database=db, host=host, user=user, password=password, port=port
    )
    return pg_conn


def get_pg_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()