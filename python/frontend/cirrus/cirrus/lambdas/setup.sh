pip2 install mmh3
mmh3_loc=`pip2 show mmh3 | grep Location`
trimmed_loc=${mmh3_loc:10}
cp -r ${trimmed_loc}/mmh* .

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
