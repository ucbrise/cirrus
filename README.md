Cirrus
==================================

[![Travis Build Status](https://travis-ci.org/jcarreira/cirrus.svg?branch=master)](https://travis-ci.org/jcarreira/cirrus)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/10708/badge.svg)](https://scan.coverity.com/projects/cirrus)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Cirrus is a serverless machine learning library. Cirrus provides a list of machine learning algorithms that can scale to many serverless lambdas in the cloud.

Requirements
============

The Cirrus backend has been tested on Ubuntu >= 14.04 and Amazon AMI.

It has been tested with the following environment / dependencies:
* g++ 6

Building
=========

    $ ./boostrap.sh
    $ make -j 10


Static analysis with Coverity
=============

Download coverity scripts (e.g., cov-analysis-linux64-8.5.0.5.tar.gz)

~~~
tar -xof cov-analysis-linux64-8.5.0.5.tar.gz
~~~

Make sure all configure.ac are setup to use C++11
~~~
cov-build --dir cov-int make -j 10

tar czvf cirrus_cov.tgz cov-int
~~~

Upload file to coverity website

Cirrus' Architecture
=============

To be done

How to Run Cirrus
=============

To be done
