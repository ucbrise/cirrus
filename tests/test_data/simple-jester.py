r = []
total = 0.0
n = 0

with open("jester_train.csv") as f:
    for line in f:
    	value = float(line.split(",")[2])
    	total += value
    	n += 1
    	r.append(value)

avg = total / n
rmse = 0
for rating in r:
	rmse += (rating - avg)**2

print((rmse / n)**(.5))