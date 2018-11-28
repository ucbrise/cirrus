""" Python script to gauge the success of a collaborative filtering model for Jester,
by comparing it with the model that guesses the average item rating for every item. """

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

print("Training RMSE: {0}".format((rmse / n)**(.5)))

test_total = 0
test_n = 0
with open("jester_test.csv") as f:
	for line in f:
		value = float(line.split(",")[2])
		test_total += (value - avg)**2
		test_n += 1

print("Test RMSE: {0}".format((test_total / test_n)**(.5)))
