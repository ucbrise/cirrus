#!/bin/bash

wget http://goldberg.berkeley.edu/jester-data/jester-data-1.zip

unzip jester-data-1.zip

rm jester-data-1.zip

mv jester-data-1.xls tests/test_data/jester-data-1.xls

sudo apt install -y python3-pip

pip3 install --user xlrd

python3 tests/test_data/parse-jester.py
