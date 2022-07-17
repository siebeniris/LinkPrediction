import pandas as pd
import os
import numpy as np
from collections import defaultdict

small = False
if small:
    with open("src/config/categories_small.txt") as f:
        categories = [line.replace("\n", "") for line in f.readlines()]
    print(f"len categories {len(categories)}")
else:
    with open("src/config/categories.txt") as f:
        categories = [line.replace("\n", "") for line in f.readlines()]
    print(f"len categories {len(categories)}")

print(categories)

# category = categories[0]
df_gold = pd.read_csv("data/frequency_ranking_new.csv")
category_gold_dict = defaultdict(list)
for k, v in df_gold.groupby("Category"):
    category_gold_dict[k] = list(tuple(zip(v["Expected Predicate"], v["Expected Object"])))

print(category_gold_dict)


def get_tf_idf_per_category(category):
    # freq, pred, obj, rank
    # 1,http://dbpedia.org/ontology/starring,http://dbpedia.org/resource/Wesley_Jonathan,786
    df_freq = pd.read_csv(f"data/preprocessed/outgoing_edges_ranked/{category}.csv")

    df_freq["sum_freq"] = [df_freq["freq"].sum() for _ in range(len(df_freq))]
    # load number of entities for each category
    df_nr_entities = pd.read_csv("data/nr_entities.csv", header=None)
    df_nr_entities.columns = ["category", "nr_of_entities"]
    nr_entities = int(df_nr_entities.loc[df_nr_entities["category"] == category]["nr_of_entities"])

    # load the occurrences of the patterns in the whole DBpedia
    df_freq_dbpedia = pd.read_csv(f"data/pattern_freq_dbpedia/{category}_count.csv", header=None)
    df_freq_dbpedia.columns = ["freq_dbpedia", "p", "o"]
    print(df_freq)
    print(df_freq_dbpedia)
    new_df = pd.merge(df_freq, df_freq_dbpedia, how="left", left_on=["p", "o"], right_on=["p", "o"])

    new_df["tf"] = new_df["freq"] / new_df["sum_freq"]
    new_df["idf"] = np.log(6670000 / new_df["freq_dbpedia"])
    new_df["tf_idf"] = new_df["tf"] * new_df["idf"]
    new_df["rank_tf_idf"] = new_df["tf_idf"].rank(method="min", ascending=False).astype("Int64")
    # new_df.to_csv(f"data/tf_idf/{category}.csv", index=False)
    tf_idf_rank_dict = {}
    for x, y, r in zip(new_df["p"], new_df["o"], new_df["rank_tf_idf"]):
        tf_idf_rank_dict[(x, y)] = r

    tuples_for_category = category_gold_dict[category]
    for tuple_ in tuples_for_category:
        rank_tf_idf_for_tuple_ = tf_idf_rank_dict[tuple_]
        print(tuple_, rank_tf_idf_for_tuple_)
        df_gold.loc[(df_gold["Category"] == category) &
                     (df_gold["Expected Predicate"] == tuple_[0]) &
                     (df_gold["Expected Object"] == tuple_[1]), "Rank_tf_idf"] = rank_tf_idf_for_tuple_


if __name__ == '__main__':
    for category in categories:
        get_tf_idf_per_category(category)
    df_gold.to_csv("data/ranking_frequency_tf_idf.csv", index=False)