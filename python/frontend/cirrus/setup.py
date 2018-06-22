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
    author_email='joao [at] berkeley [dot] edu',
    # NOTE: Email on your berkeley page was like this, not sure if you're afraid of bots parsing your github
    url='https://github.com/jcarreira/cirrus',
    packages=find_packages(exclude=('tests', 'docs')),
    license=open('LICENSE'),
    long_description=open('README.md').read(),
)
