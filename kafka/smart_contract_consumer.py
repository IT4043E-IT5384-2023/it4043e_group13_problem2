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
        "topic_prefix": os.getenv("KAFKA_SM_TOPIC"),
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

def process_kafka_messages(consumer, env_vars, args):
    all_projects = {}
    all_wallets = {}
    try:
        all_projects = load_from_bucket(
            env_vars,
            os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"projects_{args.chain}.json")
        )
        all_wallets = load_from_bucket(
            env_vars,
            os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"wallets_{args.chain}.json")
        )
    except FileNotFoundError:
        print("Starting with empty projects and wallets.")

    count = 0
    for message in consumer:
        if message.value == 0:
            break
        process_message(message, all_projects, all_wallets, count)

        if (count + 1) % 100 == 0:
            save_to_bucket(
                env_vars,
                os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"projects_{args.chain}.json"),
                all_projects
            )
            save_to_bucket(
                env_vars,
                os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"wallets_{args.chain}.json"),
                all_wallets
            )

    save_final_data(env_vars, args, all_projects, all_wallets)

def process_message(message, all_projects, all_wallets, count):
    name = message.value["name"]
    prj = message.value["prj"]
    addrs = message.value["addrs"]
    if name not in all_projects:
        count += 1
        print("Received ", name)
        all_projects[name] = prj
        for addr, prj_name in addrs.items():
            if addr not in all_wallets:
                all_wallets[addr] = {prj_name: 1}
            else:
                all_wallets[addr][prj_name] = all_wallets[addr].get(prj_name, 0) + 1

def save_final_data(env_vars, args, all_projects, all_wallets):
    save_to_bucket(
        env_vars,
        os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"projects_{args.chain}.json"),
        all_projects
    )
    save_to_bucket(
        env_vars,
        os.path.join(env_vars["gcs_prefix"], "data/smart_contract", f"wallets_{args.chain}.json"),
        all_wallets
    )

def main():
    env_vars = load_environment_variables()
    args = load_args()
    consumer = KafkaConsumer(
        bootstrap_servers=[env_vars["kafka_server"]],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="smart-contract-consumer",
    )
    consumer.subscribe([env_vars["topic_prefix"] + "_" + args.chain])

    process_kafka_messages(consumer, env_vars, args)
    consumer.close()

if __name__ == "__main__":
    main()
