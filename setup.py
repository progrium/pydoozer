#!/usr/bin/env python

from setuptools import setup

setup(
    name='PyDoozerd',
    version='0.1.0',
    author='Jeff Lindsay',
    author_email='jeff.lindsay@twilio.com',
    description='doozerd client',
    packages=['doozerd'],
    install_requires=['gevent'],
    data_files=[],
)
