#!/usr/bin/env python

from setuptools import setup

setup(
    name='pydoozer',
    version='0.1.0',
    author='Jeff Lindsay',
    author_email='jeff.lindsay@twilio.com',
    description='doozer client',
    packages=['doozer'],
    install_requires=['gevent'],
    data_files=[],
)
