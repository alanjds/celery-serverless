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
