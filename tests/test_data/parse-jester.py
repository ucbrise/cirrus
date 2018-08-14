# Convert jester-data-1.xls into a CSV of (user, item, rating) pairs.

# Make train.csv and test.csv.

import xlrd
import csv
import random

book = xlrd.open_workbook("tests/test_data/jester-data-1.xls")
sheet = book.sheet_by_index(0)

train = []
test = []

print("Generating training and testing data...")
for i in range(10000):
    for j in range(1, sheet.ncols):
        cell_val = float(sheet.cell(i, j).value)
        if cell_val == 99:
            continue
        if random.random() > .8:
            test.append([i, j - 1, cell_val / 10.0])
        else:
            train.append([i, j - 1, cell_val / 10.0])

print("Writing data...")
with open('tests/test_data/jester_train.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for l in train:
        writer.writerow(l)

with open('tests/test_data/jester_test.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for l in test:
        writer.writerow(l)
