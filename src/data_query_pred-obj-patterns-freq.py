import os.path
import time

from SPARQLWrapper import SPARQLWrapper2

# load categories
with open("src/config/categories.txt") as f:
    categories = [line.replace("\n", "") for line in f.readlines()]

print(f"all the categories: {len(categories)}")

# defined_category = "Novels_by_Stephen_King"
# SELECT  COUNT(*) AS ?c   where {
# ?x  <http://dbpedia.org/ontology/genre> <http://dbpedia.org/resource/Sitcom>.
# }
# ORDER BY DESC(?c)
for defined_category in categories:
    save_file = f"data/pattern_freq_dbpedia/{defined_category}_count.csv"

    if not os.path.exists(save_file):
        # freq,p,o,rank

        sparql_query_string_1 = """
            SELECT count(?a) as ?c ?y ?z  where {
            ?a ?y ?z.
            {
            SELECT distinct ?y ?z
            WHERE {"""
        sparql_query_string_2 = f"?x dct:subject <http://dbpedia.org/resource/Category:{defined_category}> ."
        sparql_query_string_3 = """ 
        ?x ?y ?z .
FILTER(REGEX(?y,"http://dbpedia.org/ontology")) FILTER(!REGEX(?y,"http://dbpedia.org/ontology/wikiPage")) FILTER(!REGEX(?y,"http://dbpedia.org/ontology/thumbnail"))
FILTER(REGEX(?z,"http://dbpedia.org/resource/"))}}
}
ORDER BY DESC(?c)
 """

        sparql_query_string = sparql_query_string_1 + sparql_query_string_2 + sparql_query_string_3

        print(sparql_query_string)

        sparql = SPARQLWrapper2("http://dbpedia.org/sparql")
        sparql.setQuery(sparql_query_string)

        try:
            results = sparql.query()
            writer = open(save_file, "a+")

            print(results.variables)
            if ("c", "y", "z") in results:
                bindings = results["c", "y", "z"]
                for b in bindings:
                    c = b["c"].value
                    y = b["y"].value
                    z = b["z"].value.replace("\n", "").replace("\r", "")

                    print(c, y, z)
                    writer.write(f"\"{c}\",\"{y}\",\"{z}\"\n")
            writer.close()

        except Exception as e:
            print(e)
    time.sleep(5)
