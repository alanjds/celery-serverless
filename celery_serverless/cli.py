# -*- coding: utf-8 -*-

"""Console script for celery_worker_serverless."""
import logging
import sys
import click
from celery.bin.base import Command

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')

CONTEXT_SETTINGS = dict(
    allow_extra_args=True,
    allow_interspersed_args=True,
    ignore_unknown_options=True,
)


## TODO: Handle the `celery serverless --help` call
@click.command(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def serverless(ctx, *args, **kwargs):
    logger.debug('serverless:\n\tctx: %s \n\targs: %s \n\tkwargs: %s \n\tctx.obj: %s\n', ctx, args, kwargs, ctx.obj)
    click.echo("Replace this message by putting your code into "
               "celery_worker_serverless.cli.main")
    click.echo("See click documentation at http://click.pocoo.org/")


class MainCommand(Command):
    def add_arguments(self, parser):
        parser.add_argument('options', nargs='?')   # catch-all to let Click handle the meat

    def run(self, *args, **kwargs):
        logger.debug('MainCommand:run:\n\tself: %s \n\targs: %s \n\tkwargs: %s \n' % (self, args, kwargs))
        sys.exit(serverless.main(standalone_mode=False, obj=kwargs, **CONTEXT_SETTINGS))


main = serverless

if __name__ == "__main__":
    sys.exit(serverless())  # pragma: no cover
