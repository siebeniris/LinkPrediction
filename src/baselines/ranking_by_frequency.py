import os
import pandas as pd
from collections import defaultdict

with open("src/config/categories.txt") as f:
    categories = [line.replace("\n", "") for line in f.readlines()]
print(f"len categories {len(categories)}")

df_gold = pd.read_csv("data/frequency.csv")
# 'Red_Hot_Chili_Peppers_songs':
# [('http://dbpedia.org/ontology/artist', 'http://dbpedia.org/resource/Red_Hot_Chili_Peppers')],
category_gold_dict = defaultdict(list)
for k, v in df_gold.groupby("Category"):
    category_gold_dict[k] = list(tuple(zip(v["Expected Predicate"], v["Expected Object"])))

print(category_gold_dict)

freq_list = []
for category in categories:
    print(f"Category => {category}")
    data_file = f"data/outgoing_edges_cleaned/{category}.csv"
    df = pd.read_csv(data_file, header=None)
    df.columns = ["freq", "p", "o"]
    tuples_for_category = category_gold_dict[category]
    for tuple_ in tuples_for_category:
        try:
            df["freq"] = df["freq"].astype('Int64')
            print(df["freq"].tolist()[:10])
            df["rank"] = df["freq"].rank(method="min", ascending=False).astype("Int64")
            print(df.head())
            df.to_csv(f"data/preprocessed/outgoing_edges_ranked/{category}.csv", index=False)

            total = df["freq"].sum()
            print(total)
            # count_series = df.groupby(["p", "o"]).size()
            # count_df = count_series.to_frame(name="size").reset_index()
            # count_df["rank"] = count_df["size"].rank(method="first")
            count_dict = {}
            for x,y,z,r in zip(df["p"], df["o"], df["freq"], df["rank"]):
                count_dict[(x,y)] = (z,r)
            # print(count_dict)
            freq, rank = count_dict[tuple_]
            print(f"freq {freq} and rank {rank}")
            freq_list.append((category, f"\"{tuple_[0]}\"", f"\"{tuple_[1]}\"", str(freq), str(rank), str(total), str(len(df))))

        except Exception as e:
            df["freq"] = df["freq"].astype('Int64')
            total = df["freq"].sum()
            freq_list.append((category, f"\"{tuple_[0]}\"", f"\"{tuple_[1]}\"", str(0), str(0), str(total), str(len(df)))
                             )
            print(f"{tuple_} not inside the document")


print(freq_list)
with open("data/frequency_ranking_new.csv", "w") as f:
    f.write("Category,Expected Predicate,Expected Object,Frequency,Rank,TotalFreq,Nr_p_o \n")
    for freq_ in freq_list:
        print(freq_)
        f.write(",".join(freq_)+'\n')
