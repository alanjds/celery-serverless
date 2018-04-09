#coding: utf-8
import os
import sys
from textwrap import dedent
from pkg_resources import resource_filename, Requirement
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
    _install_serverless_yml()

    commands = [
        'serverless plugin install -n serverless-python-requirements --verbose --color',
    ]
    for command in commands:
        for line, retcode in run(command):
            click.echo(line, nl=False)
        if retcode != 0:
            raise RuntimeError('Command failed: %s' % command)
    return retcode


def _install_serverless_yml():
    local_serverless_yml = './serverless.yml'
    baked_serverless_yml = resource_filename(
        Requirement.parse("celery_serverless"),
        "celery_serverless/data/serverless.yml",
    )

    should_copy_to = os.getcwd()
    if os.path.exists(local_serverless_yml):
        if click.confirm("Should I replace the existing 'serverless.yml' file?"):
            click.echo("Replacing your 'serverless.yml'", err=True)
        else:
            # TODO: Should I validate the existing one?
            click.echo("Not changing your 'serverless.yml'", err=True)
            click.secho("Please merge the following snippet manually:", blink=True, bold=True, err=True)
            click.secho(dedent("""
            # serverless.yml
            functions:
              worker:
                handler: celery_serverless.handler.worker
            """), bold=True, err=True)
            should_copy_to = os.path.join(should_copy_to, 'serverless.yml.celery')

    if should_copy_to:
        shutil.copy(baked_serverless_yml, should_copy_to)
