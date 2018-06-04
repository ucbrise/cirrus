import os
import sys

path = "nf_dataset/training_set"

ratings = {}
count = 0

for f in os.listdir(path):
    print("count: ", count, " file: ", f)
    count += 1

    file_path = path + "/" + f
    with open(file_path) as fin:
        lines = fin.readlines();

        movieId = int(lines[0].split(":")[0])
        #print("movieId: ", movieId)

        for i in range(1, len(lines)):
            #print(lines[i])
            userId, rating, asd = lines[i].split(",")
            #print("userId: ", userId, " rating: ", rating)

            if str(userId) in ratings:
                ratings[str(userId)].append((movieId, rating))
            else:
                ratings[str(userId)] = list()
                ratings[str(userId)].append((movieId, rating))


fout = open("nf_parsed", "w")

for key in ratings:
    for rats in ratings[key]:
        desc = key + "," + str(rats[0]) + "," + str(rats[1]) + "\n"
        fout.write(desc)

sys.exit(0)
