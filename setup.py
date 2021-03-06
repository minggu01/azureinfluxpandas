import os
from setuptools import setup

def read(file_name):
    '''
    Utility function to read the README file.
    Used for the long_description.  It's nice, because now 1) we have a top level
    README file and 2) it's easier to type in the README file than to put a raw
    string in below ...
    :param file_name:
    :return:
    '''
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()


setup(
    name="datalake-influx-panda",
    version="0.0.1",
    author='Statoil ASA',
    author_email="Name@statoil.com",
    description="Time series DS pipeline",
    long_description=open('README.md').read(),
    packages=[],
    package_dir={'': 'src'},
    test_suite='tests',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ], install_requires=[]
)
