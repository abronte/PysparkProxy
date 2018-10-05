#!/usr/bin/env python
import os
from distutils.core import setup

with open('requirements.txt') as fp:
    requires = fp.read().split('\n')

with open('LICENSE') as fp:
    license = fp.read()

with open('README.rst') as fp:
    readme = fp.read()

setup(
    name='PysparkProxy',
    version='0.0.6',
    packages=[
        'pyspark_proxy',
        'pyspark_proxy.server',
        'pyspark_proxy.sql'],
    license='Apache 2.0',
    description='Seamlessly execute pyspark code on remote clusters',
    long_description=readme,
    install_requires=requires,
    python_requires='>=2.7',
    url='https://github.com/abronte/PysparkProxy',
    author='Adam Bronte',
    author_email='adam@bronte.me',
)
