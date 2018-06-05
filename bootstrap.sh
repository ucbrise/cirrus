#!/bin/bash

# get third party libs
git submodule init
git submodule update

# install eigen, keyutils, kerberos, sparsehash, gflags, glog
cd third_party

if [ ! -d "eigen_source" ]; then
  sh get_eigen.sh
fi

# get keyutils library
KEYUTILS=keyutils-1.5.10
wget http://people.redhat.com/~dhowells/keyutils/$KEYUTILS.tar.bz2
tar xjf $KEYUTILS.tar.bz2
rm $KEYUTILS.tar.bz2
cd $KEYUTILS
make -j 10

# get kerberos
cd ..
KRBLIB='krb5-1.15.2'
wget https://kerberos.org/dist/krb5/1.15/$KRBLIB.tar.gz
tar xzf $KRBLIB.tar.gz
rm $KRBLIB.tar.gz
mv $KRBLIB kerberos
cd kerberos/src
./configure --disable-shared --enable-static
make -j 10
cd lib
for i in *.a; do
   ar -x $i
done
ar -qc liball.a *.o # package all .a
find . -name \*.o -delete
cd ..
cd .. # back to third_party

#compile gflags
cd ../gflags
cmake ../gflags -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON
make -j 10

#compile glog
cd ../glog
cmake ../glog
make -j 10

cd ../curl
cmake . -DCURL_STATICLIB=ON
make -j 10

#compile aws-sdk-cpp
cd ../aws-sdk-cpp
cmake ../aws-sdk-cpp -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core"
make -j 10
cd .. # back to aws-sdk-cpp

cd ../.. # back to top_dir

# main compilation
touch config.rpath
autoreconf
automake --add-missing
autoreconf
./configure
