import os
import json
from argparse import ArgumentParser
from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils import get_gc_bucket, write_gc_json_blob, read_gc_json_blob

def load_environment_variables():
    load_dotenv()
    return {
        "kafka_server": os.getenv("KAFKA_SERVER"),
        "topic_prefix": os.getenv("KAFKA_TWITTER_TOPIC"),
        "gcs_prefix": os.getenv("GCS_PREFIX")
    }

def load_args():
    parser = ArgumentParser()
    valid_chains = [
        "chain_0x1", "chain_0x38", "chain_0x89", "chain_0xfa",
        "chain_0xa4b1", "chain_0xa", "chain_0xa86a"
    ]
    parser.add_argument("--chain", type=str, required=True, choices=valid_chains)
    return parser.parse_args()

def save_to_bucket(env_vars, blob_name, data):
    bucket = get_gc_bucket()
    write_gc_json_blob(bucket, blob_name, data)

def load_from_bucket(env_vars, blob_name):
    bucket = get_gc_bucket()
    return read_gc_json_blob(bucket, blob_name)

def consume_kafka_messages(env_vars, chain):
    consumer = KafkaConsumer(
        bootstrap_servers=[env_vars["kafka_server"]],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="smart-contract-consumer"
    )
    topic = f"{env_vars['topic_prefix']}_{chain}"
    consumer.subscribe([topic])
    return consumer

def process_messages(consumer, env_vars, chain):
    all_tweets = {}
    try:
        all_tweets = load_from_bucket(
            env_vars,
            os.path.join(env_vars["gcs_prefix"], "data/tweet", f"tweets_{chain}.json")
        )
    except FileNotFoundError:
        print("Starting with an empty tweet collection.")

    count = 0
    for message in consumer:
        if "end" in message.value and abs(message.value["end"] - 1) < 1e-6:
            break
        for id, tweet in message.value["tweets"].items():
            if id not in all_tweets:
                count += 1
                all_tweets[id] = tweet
            if count % 100 == 0:
                save_to_bucket(
                    env_vars,
                    os.path.join(env_vars["gcs_prefix"], "data/tweet", f"tweets_{chain}.json"),
                    all_tweets
                )

    consumer.close()
    save_to_bucket(
        env_vars,
        os.path.join(env_vars["gcs_prefix"], "data/tweet", f"tweets_{chain}.json"),
        all_tweets
    )

def main():
    env_vars = load_environment_variables()
    args = load_args()
    consumer = consume_kafka_messages(env_vars, args.chain)
    process_messages(consumer, env_vars, args.chain)

if __name__ == "__main__":
    main()
