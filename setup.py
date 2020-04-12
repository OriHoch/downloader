from setuptools import setup, find_packages
from os import path
from time import time

here = path.abspath(path.dirname(__file__))

if path.exists("VERSION.txt"):
    # this file can be written by CI tools (e.g. Travis)
    with open("VERSION.txt") as version_file:
        version = version_file.read().strip().strip("v")
else:
    version = str(time())

setup(
    name='downloader',
    version=version,
    description='''File downloader service''',
    url='https://github.com/OriHoch/downloader',
    author='''Ori Hoch''',
    license='MIT',
    packages=find_packages(exclude=['examples', 'tests', '.tox']),
    entry_points={
      'console_scripts': [
        'downloader = downloader.cli:main',
      ]
    },
)
