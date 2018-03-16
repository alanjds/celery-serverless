#coding: utf-8
import functools

import click


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
