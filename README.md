Cirrus
==================================

[![Travis Build Status](https://travis-ci.org/jcarreira/cirrus.svg?branch=master)](https://travis-ci.org/jcarreira/cirrus)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/10708/badge.svg)](https://scan.coverity.com/projects/jcarreira-cirrus)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Cirrus is a serverless machine learning library. Cirrus provides a list of machine learning algorithms that can scale to many serverless lambdas in the cloud.

Requirements
============

The Cirrus backend has been tested on Ubuntu 14.04/16.04/18.04 and Amazon AMI.

It has been tested with the following environment / dependencies:
* g++-7

In the Amazon AMI please do:

    $ sudo yum install glibc-static
    $ sudo yum install openssl-static.x86_64
    $ sudo yum install zlib-static.x86_64

In Ubuntu please do:

    $ sudo apt-get install build-essential cmake automake zlib1g-dev libssl-dev libcurl4-nss-dev bison libldap2-dev libkrb5-dev

Building
=========

    $ ./bootstrap.sh
    $ make -j 10
