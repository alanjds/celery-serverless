#coding: utf-8
import functools
import subprocess

import click


def fix_celery_command_name(ctx, param, value):
    if ctx.info_name == 'celery':
        return value[1:]    # wipe the subcommand wrongly detected as extra argument
    return value


_accept_extra_arguments = click.argument('extra', nargs=-1, callback=fix_celery_command_name)  # Extra arguments from Celery (not options)


def click_handle_celery_options(accept_extra=True):
    """Handle the Celery default options properly, not on ctx.obj"""
    if callable(accept_extra):
        # Is not a bool. Is the final function!
        func = accept_extra
        accept_extra = True
    else:
        func = None

    def _decorator(fn):
        @click.option('-A', '--app')
        @click.option('-b', '--broker')
        @click.option('--loader')
        @click.option('--config')
        @click.option('--workdir')
        @click.option('--no-color', '-C', is_flag=True)
        @click.option('--quiet', '-q', is_flag=True)
        @functools.wraps(fn)
        def _fn(*args, **kwargs):
            return fn(*args, **kwargs)
        return _accept_extra_arguments(_fn) if accept_extra else _fn
    return _decorator(func) if func else _decorator


def run(command, *args, **kwargs):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT,
                         *args, **kwargs)
    for line in iter(p.stdout.readline, b''):
        yield (line, p.poll())  # (bytes, exitcode)
