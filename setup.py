#!/usr/bin/python3

try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

import os
import sys
import distributor

install_requires = [
	"klein>=15.2.0",
	"twisted>=15.5.0"
]

setup(
	name="distributor",
	version=distributor.__version__,
	description="Reads a newline-delimited file and hands out one line at a time to clients that make requests over a simple HTTP POST interface.",
	url="https://github.com/ludios/distributor",
	author="Ivan Kozik",
	author_email="ivan@ludios.org",
	classifiers=[
		"Programming Language :: Python :: 2",
		"Development Status :: 3 - Alpha",
		"Intended Audience :: Developers",
		"Intended Audience :: System Administrators",
		"License :: OSI Approved :: MIT License",
		"Topic :: System :: Distributed Computing",
	],
	scripts=["distributor"],
	modules=["distributor.py"],
	install_requires=install_requires
)
