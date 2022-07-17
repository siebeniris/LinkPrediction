# Dataset 3: How many combinations do we have for every category?
# Dataset 4: N of all the pairs in the whole DBpedia / pairs in our dataset.
import json
from itertools import chain

import numpy as np
import pandas as pd
import os

with open("src/config/categories.txt") as f:
    categories = [line.replace("\n", "") for line in f.readlines()]

print(f"all the categories: {len(categories)}")



def get_stats(category, data_type):

    if data_type == "comb":
        csv_file = f"data/combinations_csv/{category}.csv"
        entities_file = f"data/combinations_csv/entities/{category}.json"
        patterns_file = f"data/combinations_csv/combinations/{category}.json"
    if data_type == "triples":
        csv_file = f"data/triples_in_category/{category}.csv"
        entities_file = f"data/triples_in_category/entities/{category}.json"
        patterns_file = f"data/triples_in_category/patterns/{category}.json"

    df = pd.read_csv(csv_file, header=None)
    N = len(df) # 1. nr. of combinations for each category


    with open(entities_file) as f:
        entities = json.load(f)

    with open(patterns_file) as f:
        patterns = json.load(f)

    # 2. total/avg nr. of pred-obj pairs in each category
    list_of_patterns= list(patterns.values())

    avg_nr_pairs_object = int(np.average([len(x) for x in list_of_patterns]))
    print("average nr of pairs per obejct: ", avg_nr_pairs_object )
    pred_obj_pairs_orig = [tuple(x) for x in list(chain.from_iterable(list_of_patterns))]

    pred_obj_pairs = list(set(pred_obj_pairs_orig))

    # 3. nr. of subjects in each category
    nr_subjects = len(patterns)
    print("nr of subjects: ",nr_subjects)


    # 4. nr. of (subj-obj) pairs in each category
    nr_subj_obj_pairs = len(entities)
    print("nr of subj-obj pairs", nr_subj_obj_pairs)

    save_file.write(f"{category},{N},{nr_subjects},{len(pred_obj_pairs)},{avg_nr_pairs_object},{nr_subj_obj_pairs}\n")



if __name__ == '__main__':
    data_type=["comb", "triples"]
    header = "Category,NrTriple,NrSubj,NrPred-Obj,AvgPairPerSubj,NrSubjObjPair\n"


    for t in data_type:
        save_file = open(f"data/stats_file_{t}.csv", "a+")
        save_file.write(header)

        for category in categories:
            get_stats(category, t)

        save_file.close()



