import os
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from dotenv import load_dotenv

load_dotenv()
CMC_API_KEY = os.getenv("CMC_API_KEY")


def get_listing_coin_ids():
    listing_api = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"

    params = {
        "start": 1,
        "end": 100,
        "sortBy": "market_cap",
        "sortType": "desc",
        "convert": "USD,BTC,ETH",
        "cryptoType": "all",
        "tagType": "all",
        "audited": "false",
        "aux": "ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,circulating_supply,total_supply,volume_7d,volume_30d",
    }

    ids = []
    with Session() as s:
        s.headers[
            "User-Agent"
        ] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
        res = s.get(listing_api, params=params)
        for item in res.json()["data"]["cryptoCurrencyList"]:
            ids.append(item["id"])

    return ids


def get_coins_metadata(ids):
    metadata_api = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": CMC_API_KEY,
    }

    with Session() as s:
        s.headers.update(headers)

        for ids_chunk in [ids[i : i + 10] for i in range(0, len(ids), 10)]:
            print("Getting metadata for ids: ", ids_chunk)
            parameters = {"id": ",".join([str(x) for x in ids_chunk])}

            try:
                response = s.get(metadata_api, params=parameters)
                data = json.loads(response.text)

                # If the file is empty, add the data
                if not os.path.isfile("coins_metadata.json"):
                    with open("coins_metadata.json", "w") as f:
                        json.dump(data, f, indent=4)

                # If the file is not empty, update the data
                else:
                    with open("coins_metadata.json", "r+") as f:
                        file_data = json.load(f)
                        file_data["data"].update(data["data"])
                        f.seek(0)
                        json.dump(file_data, f, indent=4)

            except (ConnectionError, Timeout, TooManyRedirects) as e:
                print(e)


if __name__ == "__main__":
    ids = get_listing_coin_ids()
    get_coins_metadata(ids)
