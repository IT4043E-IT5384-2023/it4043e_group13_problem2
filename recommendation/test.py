import json
import pandas as pd
token_description = {}
user_token_dict = json.load(open("./wallets_info.json"))
df = pd.read_csv("./data.csv")
print(df.columns)
for token, des  in zip(df['PROJECT'], df['Unnamed: 3']):
    token_description[token] = des 

print(user_token_dict.keys())
sample_value = list(user_token_dict.values())[0][0]
print(sample_value)
print(token_description[sample_value])
