"""
0x591e22572482411db06708fcc57781c2f0fd20bc
This is a simple application for sentence embeddings: semantic search

We have a corpus with various sentences. Then, for a given query sentence,
we want to find the most similar sentence in this corpus.

This script outputs for various queries the top 5 most similar sentences in the corpus.
"""
from sentence_transformers import SentenceTransformer, util
import torch
import pickle
import argparse
import json
import pandas as pd
token_description = {}
user_token_dict = json.load(open("./wallets_info.json"))
df = pd.read_csv("./data.csv")
print(df.columns)
for token, des  in zip(df['PROJECT'], df['Unnamed: 3']):
    token_description[token] = des

# Create an argument parser
parser = argparse.ArgumentParser()
parser.add_argument('--wallet_address', type=str, required=True, help='The input file.')
args = parser.parse_args()
wallet_address = args.wallet_address

#embedding
embedder = SentenceTransformer('all-MiniLM-L6-v2')
data = pickle.load(open("./output.pkl", "rb"))
# Corpus with example sentences
#corpus = ['A man is eating food.',
#          'A man is eating a piece of bread.',
#          'The girl is carrying a baby.',
#          'A man is riding a horse.',
#          'A woman is playing violin.',
#          'Two men pushed carts through the woods.',
#          'A man is riding a white horse on an enclosed ground.',
#          'A monkey is playing drums.',
#          'A cheetah is running behind its prey.'
#          ]

corpus = [sample['text'] for sample in data]

corpus_embeddings = torch.concat([torch.tensor(sample['embeddings']).unsqueeze(0) for sample in data], dim=0).cpu()
print("???", corpus_embeddings.shape)
# Query sentences:
#queries = ['A man is eating pasta.', 'Someone in a gorilla costume is playing a set of drums.', 'A cheetah chases prey on across a field.']
queries = [token_description[token] for token in user_token_dict[wallet_address]]
# Find the closest 5 sentences of the corpus for each query sentence based on cosine similarity
top_k = min(5, len(corpus))
for query in queries:
    query_embedding = embedder.encode(query, convert_to_tensor=True).cpu()

    # We use cosine-similarity and torch.topk to find the highest 5 scores
    cos_scores = util.cos_sim(query_embedding, corpus_embeddings)[0]
    top_results = torch.topk(cos_scores, k=top_k)

    print("\n\n======================\n\n")
    print("Token in projects:", query)
    print("\nTop 5 most similar tweets in db:")

    for score, idx in zip(top_results[0], top_results[1]):
        print("Tweet: ", corpus[idx], "(Score: {:.4f})".format(score))

    """
    # Alternatively, we can also use util.semantic_search to perform cosine similarty + topk
    hits = util.semantic_search(query_embedding, corpus_embeddings, top_k=5)
    hits = hits[0]      #Get the hits for the first query
    for hit in hits:
        print(corpus[hit['corpus_id']], "(Score: {:.4f})".format(hit['score']))
    """
