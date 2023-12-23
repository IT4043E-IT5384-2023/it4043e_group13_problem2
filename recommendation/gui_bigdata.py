import streamlit as st
from sentence_transformers import SentenceTransformer, util
import torch
import pickle
import json
import pandas as pd


if __name__ == "__main__":

    token_description = {}
    user_token_dict = json.load(open("./wallets_info.json"))
    df = pd.read_csv("./data.csv")

    for token, des  in zip(df['PROJECT'], df['Unnamed: 3']):
        token_description[token] = des

    embedder = SentenceTransformer('all-MiniLM-L6-v2')
    data = pickle.load(open("./output.pkl", "rb"))

    corpus = [sample['text'] for sample in data]
    corpus_embeddings = torch.concat([torch.tensor(sample['embeddings']).unsqueeze(0) for sample in data], dim=0).cpu()

    st.markdown(f"## Tweets recommender")
    wallet_address = st.text_input("Wallet address", value='0x591e22572482411db06708fcc57781c2f0fd20bc', placeholder='0x...')
    
    if wallet_address:
        tokens = user_token_dict[wallet_address]
        queries = [token_description[token] for token in user_token_dict[wallet_address]]

        top_k = min(3, len(corpus))
        for token, query in zip(tokens, queries):
            query_embedding = embedder.encode(query, convert_to_tensor=True).cpu()

            # We use cosine-similarity and torch.topk to find the highest 5 scores
            cos_scores = util.cos_sim(query_embedding, corpus_embeddings)[0]
            top_results = torch.topk(cos_scores, k=top_k)

            print("\n\n======================\n\n")
            print("Token in projects:", query)
            print("\nTop 3 most similar tweets in db:")

            # NOI DUNG:
            project_name = token
            project_desc_md = query

            st.markdown(f"""Tweet recommendations for address:
                
                {wallet_address}""")
            st.markdown(f"### Most relevant project: **{project_name}**")
            st.markdown(project_desc_md)

            for i, score, idx in zip(range(1, 4), top_results[0], top_results[1]):
                print("Tweet: ", corpus[idx], "(Score: {:.4f})".format(score))
                tweet = corpus[idx]
                # ----------------------------------------
                st.markdown(f"### Tweet #{i}")
                st.markdown(f"{tweet}")