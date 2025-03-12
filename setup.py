#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The setup script."""
import sys
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'Click>=7.0,<8.0',
    'celery>=4.2.1,<4.4.0',
    'ruamel.yaml>=0.15.71,<0.17.0',
    'future-thread~=1.0',
    'redis>=3.2.0,<4.0',
    'backoff>=1.6.0,<2.0',
    'timeoutcontext>=1.2.0',
]

setup_requirements = []
test_requirements = ['pytest<3.7.0', 'pytest-cov<2.6.0', 'coverage<5.0']

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
        'Programming Language :: Python :: 3.8',
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
    extras_require={
        'boto3': [
            'boto3<=1.7.58,>=1.4.7',
            'botocore<2.0',
            'aioboto3>=4.1.2,<5.0',
        ],
        'wdb': [
            'wdb>=3.2.4,<4.0',
        ],
        'logdrain': [
            'raven>=6.9.0,<7.0',
        ],
        'sentry': [
            'raven>=6.9.0,<7.0',
        ],
        's3conf': [
            's3conf>=0.8.4,<1.0',
        ],
    },
    license="Apache Software License 2.0",
    long_description=readme,
    include_package_data=True,
    keywords='celery_serverless',
    name='celery-serverless',
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/alanjds/celery-serverless',
    version='0.2.1',
    python_requires='>=3.8',
    zip_safe=False,
)
