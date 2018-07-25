# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


setup(
    name='cirrus',
    version='0.1dev',
    description='Python library for Cirrus',
    author="João Carreira",
    author_email='joao.berkeley.edu',
    url='https://github.com/jcarreira/cirrus',
    packages=find_packages(exclude=('tests', 'docs')),
    license=open('LICENSE'),
    long_description=open('README.md').read(),
)
