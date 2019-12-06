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

Paper
=========

This project is part of a research project on Serverless Machine Learning Workflows. This works has been published and can be found here:

[Joao Carreira, Pedro Fonseca, Alexey Tumanov, Andrew Zhang, Randy Katz.
In the ACM Symposium on Cloud Computing 2019 (SoCC'19)](https://people.eecs.berkeley.edu/~joao/p13-Carreira.pdf "Cirrus paper")

Funding
=========

This work has been generously supported by AWS Cloud Research, FCT (Portuguese Science Foundation), NSF CISE Expeditions Award CCF-1730628, and gifts from Alibaba, Amazon Web Services, Ant Financial, CapitalOne, Ericsson, Facebook, Futurewei, Google, Intel, Microsoft, Nvidia, Scotiabank, Splunk and VMware.

Contributors
=========

Joao Carreira, Andrew Zhang, Jeff Yu, Ryan Wang, Neel Somani, Shea Conlon, Andy Wang, Pedro Fonseca, Alexey Tumanov, Randy Katz
