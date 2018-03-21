# -*- coding: utf-8 -*-

"""Console script for celery_worker_serverless."""
import logging
import sys
import argparse

import click
from celery.bin.base import Command

from .cli_utils import click_handle_celery_options
from . import deployer


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


## TODO: Handle the `celery serverless --help` call
@click.group(no_args_is_help=True, context_settings=dict(allow_interspersed_args=True))
@click_handle_celery_options(accept_extra=False)
def serverless(*args, **kwargs):
    pass


@serverless.command('serverless')
@click_handle_celery_options
@click.pass_context
def _serverless(ctx, *args, **kwargs):
    """
    Boilerplate to handle the `celery serverless` command.
    Should not be used by humans on the CLI directly.
    """
    logger.debug('_serverless:\n\tctx: %s \n\targs: %s \n\tkwargs: %s \n\tctx.obj: %s\n', ctx, args, kwargs, ctx.obj)
    sys.argv.pop(sys.argv.index('serverless'))  # Call again w/ one depth less
    sys.exit(serverless.main(prog_name='celery serverless', standalone_mode=True, obj=kwargs, allow_interspersed_args=True))


@serverless.command()
@click_handle_celery_options
@click.pass_context
def deploy(ctx, *args, **kwargs):
    sys.exit(deployer.deploy())


@serverless.command()
@click_handle_celery_options
@click.pass_context
def init(ctx, *args, **kwargs):
    sys.exit(deployer.init_serverless())


class MainCommand(Command):
    def add_arguments(self, parser):
        parser.add_argument('subcommand', nargs='*')   # catch-all to let Click handle the meat
        # parser.add_argument('--spam', nargs='?')

    def run(self, *args, **kwargs):
        logger.debug('MainCommand:run:\n\tself: %s \n\targs: %s \n\tkwargs: %s \n' % (self, args, kwargs))
        sys.exit(serverless.main(standalone_mode=True, obj=kwargs, allow_interspersed_args=True))


main = serverless

if __name__ == "__main__":
    sys.exit(serverless())  # pragma: no cover
