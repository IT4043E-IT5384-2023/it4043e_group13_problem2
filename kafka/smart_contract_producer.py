import os
import json
from argparse import ArgumentParser
from kafka import KafkaProducer
from dotenv import load_dotenv
from utils import get_gc_bucket, read_gc_json_blob, get_pg_conn, get_mg_client, get_chain_db
import psycopg2
import time
from tqdm import tqdm

load_dotenv()


class SmartContractCrawler:
    def __init__(self, chain):
        self.chain = chain
        self.connect()

    def connect(self):
        env_vars = self.load_environment_variables()
        self.pg_conn = get_pg_conn(
            env_vars["postgres_db"],
            env_vars["postgres_host"],
            env_vars["postgres_user"],
            env_vars["postgres_password"],
            env_vars["postgres_port"],
        )
        mongo_client = get_mg_client(env_vars["mongo_conn_str"])
        mongo_kg_client = get_mg_client(env_vars["mongo_kg_conn_str"])
        self.transaction_db = mongo_client[get_chain_db(self.chain)].transactions
        self.projects_db = mongo_kg_client.knowledge_graph.projects

    @staticmethod
    def load_environment_variables():
        return {
            "postgres_db": os.getenv("POSTGRES_DB"),
            "postgres_host": os.getenv("POSTGRES_HOST"),
            "postgres_user": os.getenv("POSTGRES_USER"),
            "postgres_password": os.getenv("POSTGRES_PASSWORD"),
            "postgres_port": os.getenv("POSTGRES_PORT"),
            "mongo_conn_str": os.getenv("MONGO_CONN_STR"),
            "mongo_kg_conn_str": os.getenv("MONGO_KG_CONN_STR"),
        }

    def get_projects(self):
        print("Querying projects ...")
        query = f"SELECT DISTINCT project FROM {self.chain}.smart_contract;"
        with self.pg_conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()

        if not res:
            print(f"No project found in {self.chain}.smart_contract")
            return []

        project_names = [r[0] for r in res]
        res = [name for name in tqdm(project_names) if self.projects_db.find_one({"_id": "-".join(name.split("_"))})]
        print(f"Found {len(res)} projects in {self.chain}.smart_contract")
        return res

    def extract(self, project_name):
        query = f"SELECT contract_address, is_good FROM {self.chain}.smart_contract WHERE project = '{project_name}';"
        with self.pg_conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()

        if not res:
            print(f"No contract address found for {project_name}")
            return None, None

        project_data = self._fetch_project_data(project_name)
        addresses = self._process_contracts(res, project_data)
        return project_data, addresses

    def _fetch_project_data(self, project_name):
        prj = self.projects_db.find_one({"_id": "-".join(project_name.split("_"))})
        if not prj:
            return None

        fields = ["name", "category", "source", "volume", "numberOfUsers", "numberOfTransactions",
                  "numberOfItems", "numberOfOwners", "transactionVolume", "tvl",
                  "socialAccounts", "rankDefi", "rankNft", "rankTVL", "lastUpdatedAt",
                  "marketShare", "marketShareNFT", "marketShareDefi"]

        project_data = {field: prj.get(field, self._default_value(field)) for field in fields}
        project_data["tvlByChains"] = prj.get("tvlByChains", {}).get(self.chain[6:], 0)
        project_data["numberOfGoodContracts"] = 0
        project_data["numberOfBadContracts"] = 0
        return project_data

    def _default_value(self, field):
        if field == "lastUpdatedAt":
            return time.time()
        elif field == "socialAccounts":
            return []
        else:
            return 0

    def _process_contracts(self, contract_results, project_data):
        addresses = {}
        for address, is_good in tqdm(contract_results):
            project_data["numberOfGoodContracts"] += is_good
            project_data["numberOfBadContracts"] += not is_good
            tran_addresses = self._get_transaction_addresses(address)
            addresses.update({addr: self.chain for addr in tran_addresses})
            project_data["numberOfContractsTransactions"] = project_data.get("numberOfContractsTransactions", 0) + len(tran_addresses)
        return addresses

    def _get_transaction_addresses(self, to_address):
        trans = self.transaction_db.find({"to_address": {"$eq": to_address}})
        return {tran["from_address"] for tran in trans}


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
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--num_producer", type=int, default=10)
    return parser.parse_args()

def get_project_names(env_vars, chain):
    bucket = get_gc_bucket()
    prj_blob_path = os.path.join(env_vars["gcs_prefix"], "data/project", f"{chain}.json")
    return list(read_gc_json_blob(bucket, prj_blob_path))

def process_project(sm_crawler, producer, topic, project_name):
    try:
        prj, addrs = sm_crawler.extract(project_name)
    except psycopg2.OperationalError:
        producer.send(topic, value={"error": "database connection error"})
        sm_crawler.connect()
        prj, addrs = sm_crawler.extract(project_name)
    data = {
        "name": project_name,
        "prj": prj,
        "addrs": addrs,
    }
    producer.send(topic, value=data)
    print(f"Sent {project_name}")

def main():
    env_vars = load_environment_variables()
    args = load_args()
    topic = env_vars["topic_prefix"] + "_" + args.chain

    sm_crawler = SmartContractCrawler(chain=args.chain)
    producer = KafkaProducer(
        bootstrap_servers=[env_vars["kafka_server"]],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    project_names = get_project_names(env_vars, args.chain)
    chunk = len(project_names) // args.num_producer
    start = args.start * chunk
    end = (args.start + 1) * chunk if args.start != args.num_producer - 1 else len(project_names)
    print(f"Producer {args.start} will process projects {start}-{end} of {len(project_names)}")

    for project_name in project_names[start:end]:
        process_project(sm_crawler, producer, topic, project_name)

    if end == len(project_names):
        producer.send(topic, value=0)
    producer.close()

if __name__ == "__main__":
    main()
