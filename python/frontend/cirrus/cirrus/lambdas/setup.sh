#!/usr/bin/env bash

pip2 install mmh3
# get path to mmh3 library
mmh3_loc=`pip2 show mmh3 | grep Location | awk '{print \$2}'`
cp -r ${mmh3_loc}/mmh* .

mkdir temp_files
cd temp_files

git clone https://github.com/Grokzen/redis-py-cluster.git
cp -r redis-py-cluster/rediscluster ../

git clone https://github.com/andymccurdy/redis-py.git
cp -r redis-py/redis ../

git clone https://github.com/uiri/toml.git
cp -r toml/toml ../

cd ../
rm -rf temp_files
