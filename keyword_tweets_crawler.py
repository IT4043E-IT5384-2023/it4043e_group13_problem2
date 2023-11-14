import asyncio
from tqdm import tqdm
import json
import os
from twscrape import API
from twscrape.logger import set_log_level
from datetime import date, datetime
from dotenv import load_dotenv
import fire

load_dotenv()
ACCOUNT_DB = os.getenv("ACCOUNT_DB")


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def save_to_file(data, filename):
    # If the file exists, load the existing data
    if os.path.exists(filename):
        with open(filename, "r") as f:
            file_data = json.load(f)

        file_data.extend(data)

        # Save the updated data
        with open(filename, "w") as f:
            json.dump(file_data, f, indent=4, default=json_serial)

    else:
        with open(filename, "w") as f:
            json.dump(data, f, indent=4, default=json_serial)


async def keyword_tweets_crawler(start=0, end=100, limit=50000):
    api = API(ACCOUNT_DB)
    set_log_level("DEBUG")

    with open("data/CMC_coins_metadata.json", "r") as f:
        data = json.load(f)
        data = data["data"]

    data = list(data.values())[start:end]

    for item in tqdm(data, total=len(data), desc="Fetching tweets"):
        print(f"Fetching tweets for {item['name']}")
        keyword = item["name"]
        filter = "since:2023-01-01 lang:en min_replies:5 min_faves:20 min_retweets:10"

        if item["name"] == item["symbol"]:
            q = f"{keyword}{filter}"
        else:
            q = f"{keyword} OR {item['symbol']} {filter}"

        tweets = []
        tweet_count = 0
        async for tweet in api.search(q, limit=limit):
            tweets.append(tweet.dict())
            tweet_count += 1

            if tweet_count % 1000 == 0:
                print(f"Collected {tweet_count} tweets for {keyword}")

                save_to_file(tweets, f"data/twitter/{keyword}_tweets.json")

                tweets = []

        save_to_file(tweets, f"data/twitter/{keyword}_tweets.json")

        print(f"Collected {tweet_count} tweets for {keyword}")


def main(start=0, end=100, limit=50000):
    asyncio.run(fire.Fire(keyword_tweets_crawler))


if __name__ == "__main__":
    main()
