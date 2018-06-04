# get third party libs
git submodule init
git submodule update

cd third_party
git clone https://github.com/redis/hiredis
make -j 4

if [ ! -d "eigen_source" ]; then
  sh get_eigen.sh
fi
cd ..

# main compilation
touch config.rpath
autoreconf
automake --add-missing
autoreconf
./configure
