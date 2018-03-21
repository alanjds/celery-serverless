#coding: utf-8
import os
import sys
from pkg_resources import resource_filename, Requirement
from pathlib import Path
import shutil

import click

from .cli_utils import run


def deploy():
    # TODO: bake the serverless.yml on the project folder from the library one
    command = 'serverless deploy --verbose --color'
    for line, retcode in run(command): #, shell=True):
        click.echo(line, nl=False)
    return retcode


def init_serverless():
    local_serverless_yml = Path('./serverless.yml')
    baked_serverless_yml = resource_filename(
        Requirement.parse("celery_serverless"),
        "celery_serverless/data/serverless.yml",
    )

    should_copy = True
    if local_serverless_yml.exists():
        if not click.confirm("Should I replace the existing 'serverless.yml' file?"):
            should_copy = False

    if should_copy:
        shutil.copy(baked_serverless_yml, os.getcwd())

    commands = [
        'serverless plugin install -n serverless-python-requirements --verbose --color',
    ]
    for command in commands:
        for line, retcode in run(command):
            click.echo(line, nl=False)
        if retcode != 0:
            raise RuntimeError('Command failed: %s' % command)
    return retcode
