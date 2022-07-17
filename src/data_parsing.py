import os
import pandas as pd
from collections import defaultdict
import json

resource_url = "http://dbpedia.org/resource/"
ontology_url = "http://dbpedia.org/ontology/"


def preprocessing_combinations(file):
    print(f"loading file: {file}")
    df = pd.read_csv(file, header=None, low_memory=False, encoding = "ISO-8859-1")
    print(f"len of df {len(df)}")
    df.columns = ["s", "p", "o"]
    df.s = df.s.str.replace(resource_url, "")
    df.p = df.p.str.replace(ontology_url, "")
    df.o = df.o.str.replace(resource_url, "")

    subj_comb_dict = defaultdict(list)
    for k, v in df.groupby("s"):
        subj_comb_dict[k] = list(tuple(zip(v["p"], v["o"])))

    entities_tuples = list(set(list(tuple(zip(df.s, df.o)))))
    print("entity tuples: ", len(entities_tuples))

    filename = os.path.basename(file).replace(".csv", "")
    # save_file = f"data/preprocessed/combinations/{filename}.json"
    save_file = f"data/triples_in_category/patterns/{filename}.json"
    with open(save_file, "w") as f:
        json.dump(subj_comb_dict, f)

    # save_file_entities = f"data/preprocessed/entities/{filename}.json"
    save_file_entities = f"data/triples_in_category/entities/{filename}.json"
    with open(save_file_entities, "w") as f:
        json.dump(entities_tuples, f)


def preprocessing_outgoing_edges(file):
    print(f"loading file: {file}")
    df = pd.read_csv(file, header=None, low_memory=False, on_bad_lines='skip')
    df.columns = ["freq", "p", "o"]
    save_folder = "data/preprocessed/outgoing_edges_cleaned"
    print(f"len {len(df)}")
    # ignore everything that is not DBpedia
    df = df[df["o"].str.contains(resource_url)==True]
    print(f"len {len(df)}")
    # ignore abstract, thumbnails
    df = df[df["o"].str.contains(ontology_url+"abstract")==False]
    df = df[df["o"].str.contains(ontology_url+"thumbnail")==False]
    filename = os.path.basename(file)
    df.to_csv(os.path.join(save_folder, filename), index=False)


if __name__ == '__main__':
    with open("src/config/categories.txt") as f:
        categories = [line.replace("\n", "") for line in f.readlines()]
    print(f"len categories {len(categories)}")
    for category in categories:
        file_ = f"data/triples_in_category/{category}.csv"
        # file_ = f"data/combinations_csv/{category}.csv"
        if os.path.exists(file_):
            preprocessing_combinations(file_)
        # file = f"data/outgoing_edges/{category}.csv"
        # preprocessing_outgoing_edges(file)

    # loading
    # file: data / outgoing_edges / English_pop_pianists.csv
    # len
    # 559
    # len
    # 227
