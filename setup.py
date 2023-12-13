from setuptools import setup, find_packages

setup(
    name='dictcruncher',
    version='0.1',
    packages=find_packages(),
    url='https://github.com/trtrojo/dictcruncher',
    license='LICENSE',
    author='Tommy Rojo',
    author_email='tr.trojo@gmail.com',
    description='A library for flattening json objects',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.9',
        'License :: OSI Approved :: MIT License'
    ]
)