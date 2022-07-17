import os.path
import time

from SPARQLWrapper import SPARQLWrapper2

#load categories
with open("src/config/categories.txt") as f:
    categories =[line.replace("\n", "") for line in  f.readlines()]

print(f"all the categories: {len(categories)}")

# defined_category = "Novels_by_Stephen_King"

for defined_category in categories:
    save_file = f"data/outgoing_edges_cleaned/{defined_category}.csv"
    if not os.path.exists(save_file):
        print(f"querying the category {defined_category} ")
        sparql_query_string_1 = """
        SELECT COUNT(?x) AS ?c ?y ?z  where { """

        sparql_query_string_2 = f"?x dct:subject <http://dbpedia.org/resource/Category:{defined_category}>."
        sparql_query_string_3 = """
        ?x ?y ?z . FILTER(REGEX(?y,"http://dbpedia.org/ontology")) 
        FILTER(REGEX(?z, "http://dbpedia.org/resource/")) 
        FILTER(!REGEX(?y,"http://dbpedia.org/ontology/wiki"))
        FILTER(!REGEX(?y,"http://dbpedia.org/ontology/wikiPage"))} 
        ORDER BY DESC(?c)
        """

        sparql_query_string = sparql_query_string_1 + sparql_query_string_2 + sparql_query_string_3

        print(sparql_query_string)

        sparql = SPARQLWrapper2("http://dbpedia.org/sparql")
        sparql.setQuery(sparql_query_string)

        try:
            results = sparql.query()
            writer = open(save_file, "w")

            print(results.variables)
            if ("c", "y", "z") in results:
                bindings = results["c", "y", "z"]
                for b in bindings:
                    x = b["c"].value
                    p = b["y"].value
                    o = b["z"].value
                    print(x,p,o)
                    writer.write(f"\"{x}\",\"{p}\",\"{o}\"\n")
            writer.close()

        except Exception as e:
            print(e)
        time.sleep(5)