import os.path
import time

from SPARQLWrapper import SPARQLWrapper2

# load categories
with open("src/config/categories_extra.txt") as f:
    categories = [line.replace("\n", "") for line in f.readlines()]

print(f"all the categories: {len(categories)}")

# defined_category = "Novels_by_Stephen_King"
# SELECT  COUNT(*) AS ?c   where {
# ?x  <http://dbpedia.org/ontology/genre> <http://dbpedia.org/resource/Sitcom>.
# }
# ORDER BY DESC(?c)
save_file = "data/nr_entities.txt"
nr_entities_list = []
for defined_category in categories:
    # save_file = f"data/nr_entities.csv"
    sparql_query_string_1 = """
            select count(?x) as ?c where {"""
    sparql_query_string_2 = f"?x dct:subject <http://dbpedia.org/resource/Category:{defined_category}>"
    sparql_query_string_3 = """} """

    sparql_query_string = sparql_query_string_1 + sparql_query_string_2 + sparql_query_string_3

    print(sparql_query_string)

    sparql = SPARQLWrapper2("http://dbpedia.org/sparql")
    sparql.setQuery(sparql_query_string)

    try:
        results = sparql.query()
        writer = open(save_file, "a+")

        print(results.variables)
        if ("c") in results:
            bindings = results["c"]
            for b in bindings:
                c = b["c"].value

                print(c)
                writer.write(f"{defined_category},\"{c}\"\n")
        writer.close()

    except Exception as e:
        print(e)
