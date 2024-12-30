from setuptools import setup
from io import open
from os import path
import sys

DESCRIPTION = "Package for writing data into an EHR through the Redox APIs from Databricks"
this_directory = path.abspath(path.dirname(__file__))

with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

VERSION = "0.0.1"
setup(
    name="redoxwrite",
    version=VERSION,
    python_requires='>=3.9',
    author="Emma Yamada, Aaron Zavora",
    description= DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/databricks-industry-solutions/redox-ehr-api",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=['redoxwrite'],
    install_requires=[
        "jwt"
    ]
)
