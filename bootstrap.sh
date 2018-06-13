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
if [ ! -f $KEYUTILS.tar.bz2 ]; then
  wget http://people.redhat.com/~dhowells/keyutils/$KEYUTILS.tar.bz2
fi
tar xjf $KEYUTILS.tar.bz2
mv $KEYUTILS keyutils
#rm $KEYUTILS.tar.bz2
cd keyutils
make -j 10

# get kerberos
cd ..
KRBLIB='krb5-1.15.2'
if [ ! -f $KRBLIB.tar.gz ]; then
  wget https://kerberos.org/dist/krb5/1.15/$KRBLIB.tar.gz
fi
tar xzf $KRBLIB.tar.gz
#rm $KRBLIB.tar.gz
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

#untar and compile curl
cd ..
mkdir curl; cd curl
CURL_TAR=curl-7.60.0.tar.gz
wget https://curl.askapache.com/$CURL_TAR
tar -xvzf $CURL_TAR
mv curl-7.60.0 curl
cd curl
./buildconf
./configure --disable-shared --enable-static  --disable-ldap --disable-sspi --without-librtmp --disable-ftp --disable-file --disable-dict --disable-telnet --disable-tftp --disable-rtsp --disable-pop3 --disable-imap --disable-smtp --disable-gopher --disable-smb --without-libidn
make -j 10
cd ../

#compile aws-sdk-cpp
cd ../aws-sdk-cpp
mkdir build
cd build
cmake .. -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core"
make -j 10
cd ../.. # back to third_party

cd .. # back to top_dir

# main compilation
touch config.rpath
autoreconf
automake --add-missing
autoreconf
./configure
