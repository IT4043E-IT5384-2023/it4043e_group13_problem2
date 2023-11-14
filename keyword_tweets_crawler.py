import asyncio
from tqdm import tqdm
import json
import os
from twscrape import API
from twscrape.logger import set_log_level
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()
ACCOUNT_DB = os.getenv("ACCOUNT_DB")


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


async def main():
    api = API(ACCOUNT_DB)
    set_log_level("DEBUG")

    with open("data/CMC_coins_metadata.json", "r") as f:
        data = json.load(f)
        data = data["data"]

    data = list(data.values())

    for item in tqdm(data, total=len(data), desc="Fetching tweets"):
        print(f"Fetching tweets for {item['name']}")
        keyword = item["name"]

        if item["name"] == item["symbol"]:
            q = f"{keyword} since:2023-01-01"
        else:
            q = f"{keyword} OR {item['symbol']} since:2023-01-01"

        tweets = []
        async for tweet in api.search(q, limit=-1):
            tweets.append(tweet.dict())

        with open(f"data/twitter/{keyword}_tweets.json", "w") as f:
            json.dump(tweets, f, indent=4, default=json_serial)


if __name__ == "__main__":
    asyncio.run(main())
