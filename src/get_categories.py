import os

writer = open("src/config/categories.txt","w")

filenames =[]
for filename in os.listdir("data/testsets"):
    filename = filename.replace("outgoing_","").replace("ingoing_", "").replace(".rdf", "")
    filenames.append(filename)

files = list(set(filenames))
writer.write("\n".join(files))
writer.close()