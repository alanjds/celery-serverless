#coding: utf-8
import sys

import click

from .cli_utils import run


def deploy():
    command = ['serverless', 'deploy', '--verbose', '--color']
    for line, retcode in run(command): #, shell=True):
        click.echo(line, nl=False)
    return retcode
