# Convert jester-data-1.xls into a CSV of (user, item, rating) pairs.

# Make train.csv and test.csv.

import xlrd
import csv
import random

book = xlrd.open_workbook("jester-data-1.xls")
sheet = book.sheet_by_index(0)

train = []
test = []

print("Generating training and testing data...")
for i in range(10000):
    for j in range(1, sheet.ncols):
        if float(sheet.cell(i, j).value) == 99:
            continue
        if random.random() > .8:
            test.append([i, j - 1, sheet.cell(i, j).value])
        else:
            train.append([i, j - 1, sheet.cell(i, j).value])

print("Writing data...")
with open('jester_train.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for l in train:
        writer.writerow(l)

with open('jester_test.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for l in test:
        writer.writerow(l)