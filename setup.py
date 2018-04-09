#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import sys
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'Click>=6.0',
    'celery~=4.1.0',
    'ruamel.yaml~=0.15.37',
]

setup_requirements = []

test_requirements = ['pytest', 'coverage']

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
if needs_pytest:
    setup_requirements += ['pytest-runner']


setup(
    author="Alan Justino & Samuel Barbosa Neto",
    author_email='alan.justino@yahoo.com.br',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="Celery worker deployed as a Serverless application",
    entry_points={
        'console_scripts': [
            'celery-serverless=celery_serverless.cli:main',
        ],
        'celery.commands': [
            'serverless = celery_serverless.cli:MainCommand',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme,
    include_package_data=True,
    keywords='celery_serverless',
    name='celery-serverless',
    packages=find_packages(include=['celery_serverless']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/alanjds/celery-serverless',
    version='0.1.0',
    zip_safe=False,
)
