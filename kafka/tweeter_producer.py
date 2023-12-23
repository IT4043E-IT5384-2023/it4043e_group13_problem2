import os
import json
from argparse import ArgumentParser
from kafka import KafkaProducer
from dotenv import load_dotenv
from utils import get_gc_bucket, read_gc_json_blob
from crawler import TweetCrawler

def load_environment_variables():
    load_dotenv()
    return {
        "kafka_server": os.getenv("KAFKA_SERVER"),
        "topic_prefix": os.getenv("KAFKA_TWITTER_TOPIC"),
        "gcs_prefix": os.getenv("GCS_PREFIX"),
        "twitter_password_path": os.getenv("TWITTER_PASSWORD_PATH")
    }

def load_twitter_passwords(password_path):
    with open(password_path) as f:
        return json.load(f)

def load_args(twitter_passwords):
    parser = ArgumentParser()
    accs = range(len(twitter_passwords))
    parser.add_argument("--acc", type=int, default=0, choices=accs)

    valid_chains = [
        "chain_0x1", "chain_0x38", "chain_0x89", "chain_0xfa",
        "chain_0xa4b1", "chain_0xa", "chain_0xa86a"
    ]
    parser.add_argument("--chain", type=str, required=True, choices=valid_chains)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--num_producer", type=int, default=10)

    return parser.parse_args()

def chain_keywords(chain):
    mapping = {
        "chain_0x1": ["ethereum", "eth"],
        "chain_0x38": ["bnb"],
        "chain_0x89": ["polygon"],
        "chain_0xfa": ["fantom", "ftm"],
        "chain_0xa4b1": ["Arbitrum One", "arb"],
        "chain_0xa": ["Optimism"],
        "chain_0xa86a": ["Avalanche C-Chain"],
    }
    return mapping[chain]

def get_keywords(project):
    keywords = [
        "_".join(project["_id"].split("-")),
        project["category"],
        project["source"],
    ]
    keywords = [k.lower() for k in keywords]
    keywords = " ".join(keywords)
    return keywords.strip().split(" ")

def process_tweets(tweets):
    p_tweets = {}
    for tweet in tweets:
        if tweet["id"] not in p_tweets and tweet._get_language() == "en":
            p_tweets[tweet["id"]] = {
                "id": tweet["id"],
                "text": tweet["text"] if tweet["text"] else "",
                "date": tweet["date"].timestamp() if tweet["date"] else 0,
                "hashtags": [str(h) for h in tweet["hashtags"]]
                if tweet["hashtags"]
                else [],
                # "views": round(float(tweet["views"])) if tweet["views"] else 0,
                "reply_counts": tweet["reply_counts"] if tweet["reply_counts"] else 0,
                "retweet_counts": tweet["retweet_counts"]
                if tweet["retweet_counts"]
                else 0,
                "likes": tweet["likes"] if tweet["likes"] else 0,
                "is_sensitive": tweet["is_sensitive"],
            }
    return p_tweets

def send_tweets(producer, topic, tweets):
    if tweets:
        data = {"tweets": process_tweets(tweets)}
        producer.send(topic, value=data)
        print(f"Sent {len(tweets)} tweets")

def main():
    env_vars = load_environment_variables()
    twitter_passwords = load_twitter_passwords(env_vars["twitter_password_path"])
    args = load_args(twitter_passwords)

    topic = env_vars["topic_prefix"] + "_" + args.chain
    tweet_crawler = TweetCrawler(args.acc)
    producer = KafkaProducer(
        bootstrap_servers=[env_vars["kafka_server"]],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    bucket = get_gc_bucket()
    prj_blob_path = os.path.join(
        env_vars["gcs_prefix"], "data/smart_contract", f"projects_{args.chain}.json"
    )
    print(prj_blob_path)
    projects = read_gc_json_blob(bucket, prj_blob_path)

    chunk_size = len(projects) // args.num_producer
    start_index = args.start * chunk_size
    end_index = (args.start + 1) * chunk_size if args.start != args.num_producer - 1 else len(projects)
    
    print(f"Producer {args.start} will produce tweets for projects {start_index}-{end_index} out of {len(projects)} total projects.")

    for project_name in list(projects.keys())[start_index:end_index]:
        keywords = get_keywords(projects[project_name])
        try:
            tweets = tweet_crawler.get_tweets_by_keywords(keywords)
            send_tweets(producer, topic, tweets)
            print(f"Sent tweets of project {project_name}")
        except Exception as e:
            print(f"Error occurred while fetching tweets for {project_name}: {e}")
            tweet_crawler.sign_in(tweet_crawler.account)

    # Process tweets related to the blockchain chain
    chain_keys = chain_keywords(args.chain)
    try:
        chain_tweets = tweet_crawler.get_tweets_by_keywords(chain_keys)
        send_tweets(producer, topic, chain_tweets)
        print(f"Sent tweets related to chain {args.chain}")
    except Exception as e:
        print(f"Error occurred while fetching chain tweets: {e}")
        tweet_crawler.sign_in(tweet_crawler.account)

    # Mark the end of the batch for this producer
    producer.send(topic, value={"end": 1 / args.num_producer})
    producer.close()

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
