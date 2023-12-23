# Portfolio Management Application

## Description

This script `keyword_tweets_crawler.py` is used to scrape tweets by keywords which are names and symbols of cryptocurrencies, as in `data\CMC_coins_metadata.json`.

## Usage

1. Install dependencies

```
pip install -r requirements.txt
```

2. Set up environment variables in `.env` file

   To use this script, you need to set up environment variables in `.env` file. All the variables are listed in `.env.example` file. There are two:

- `CMC_API_KEY`: API key for CoinMarketCap API, needed for running `coin_market_cap.py` script to collect metadata of cryptocurrencies.
- `ACCOUNT_DB`: Directory of the database file for storing Twitter account information, refer to `twscrape`'s [documentation](https://github.com/vladkens/twscrape#add-accounts--login) for more information.

3. Run the script

```
python -m keyword_tweets_crawler --start <start_idx> --end <end_idx> --limit <limit>
```

where `start_idx` and `end_idx` are the start and end index of the list of cryptocurrencies to scrape, and `limit` is the maximum number of tweets to scrape for each cryptocurrency.
