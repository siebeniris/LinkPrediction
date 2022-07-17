import os.path
import time

from SPARQLWrapper import SPARQLWrapper2

#load categories
with open("src/config/categories.txt") as f:
    categories =[line.replace("\n", "") for line in  f.readlines()]

print(f"all the categories: {len(categories)}")

# defined_category = "Novels_by_Stephen_King"

for defined_category in categories:
    save_file = f"data/triples_in_category/{defined_category}.csv"
    if not os.path.exists(save_file):
        print(f"querying the category {defined_category} ")
        sparql_query_string_1 = """
        SELECT ?s ?p ?o WHERE { """

        sparql_query_string_2 = f"?s dct:subject <http://dbpedia.org/resource/Category:{defined_category}> ."
        sparql_query_string_3 = """
        ?s ?p ?o .
        FILTER (REGEX(?p, "http://dbpedia.org/ontology")) 
        FILTER(REGEX(?o, "http://dbpedia.org/resource/")) 
        FILTER(!REGEX(?p,"http://dbpedia.org/ontology/wiki"))}
        """

        sparql_query_string = sparql_query_string_1 + sparql_query_string_2 + sparql_query_string_3

        print(sparql_query_string)

        sparql = SPARQLWrapper2("http://dbpedia.org/sparql")
        sparql.setQuery(sparql_query_string)

        try:
            results = sparql.query()
            writer = open(save_file, "w")

            print(results.variables)
            if ("s", "p", "o") in results:
                bindings = results["s", "p", "o"]
                for b in bindings:
                    s = b["s"].value
                    p = b["p"].value
                    o = b["o"].value.replace("\n","").replace("\r","")

                    print(s,p,o)
                    writer.write(f"\"{s}\",\"{p}\",\"{o}\"\n")
            writer.close()

        except Exception as e:
            print(e)
        time.sleep(5)