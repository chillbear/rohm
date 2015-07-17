import os
from setuptools import setup, find_packages

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


__version__ = None   # ignore flake8
exec(open('rohm/version.py').read())

setup(
    name="rohm",
    version=__version__,
    author="Alvin Chow",
    author_email="alvin@doordash.com",
    description=("Redis object hash mapper"),
    license="BSD",
    keywords="doordash",
    # url="http://packages.python.org/an_example_pypi_project",
    packages=find_packages(),
    scripts=[
    ],
    dependency_links = [
    ],
    install_requires=[
        'python-dateutil>=2.4.1',
        'pytz>=2014.10',
        'redis>=2.10.3',
        'six>=1.9.0',
    ],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        "License :: OSI Approved :: BSD License",
    ],
)
