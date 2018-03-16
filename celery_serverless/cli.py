# -*- coding: utf-8 -*-

"""Console script for celery_worker_serverless."""
import logging
import sys
import functools
import argparse

import click
from celery.bin.base import Command

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

CONTEXT_SETTINGS = dict(
    allow_interspersed_args=True,
)


def fix_celery_command_name(ctx, param, value):
    if ctx.info_name == 'celery':
        return value[1:]    # wipe the subcommand wrongly detected as extra argument
    return value


def click_handle_celery_options(fn):
    """Handle the Celery default options properly, not on ctx.obj"""
    @click.option('-A', '--app')
    @click.option('-b', '--broker')
    @click.option('--loader')
    @click.option('--config')
    @click.option('--workdir')
    @click.option('--no-color', '-C', is_flag=True)
    @click.option('--quiet', '-q', is_flag=True)
    @click.argument('extra', nargs=-1, callback=fix_celery_command_name)  # Extra arguments from Celery (not options)
    @functools.wraps(fn)
    def _fn(*args, **kwargs):
        return fn(*args, **kwargs)
    return _fn


## TODO: Handle the `celery serverless --help` call
@click.command(context_settings=CONTEXT_SETTINGS)
@click_handle_celery_options
@click.pass_context
def serverless(ctx, *args, **kwargs):
    logger.debug('serverless:\n\tctx: %s \n\targs: %s \n\tkwargs: %s \n\tctx.obj: %s\n', ctx, args, kwargs, ctx.obj)
    click.echo("Replace this message by putting your code into "
               "celery_worker_serverless.cli.main")
    click.echo("See click documentation at http://click.pocoo.org/")


class MainCommand(Command):
    def add_arguments(self, parser):
        parser.add_argument('options', nargs='*')   # catch-all to let Click handle the meat
        # parser.add_argument('--spam', nargs='?')

    def run(self, *args, **kwargs):
        logger.debug('MainCommand:run:\n\tself: %s \n\targs: %s \n\tkwargs: %s \n' % (self, args, kwargs))
        sys.exit(serverless.main(obj=kwargs, **CONTEXT_SETTINGS))


main = serverless

if __name__ == "__main__":
    sys.exit(serverless())  # pragma: no cover
