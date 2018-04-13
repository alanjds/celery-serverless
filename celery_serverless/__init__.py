# -*- coding: utf-8 -*-

"""Top-level package for celery-worker-serverless."""

__author__ = """Alan Justino & Samuel Barbosa Neto"""
__email__ = 'alan.justino@yahoo.com.br'
__version__ = '0.2.0'


# Hack: Allow `sls invoke local` to work correctly
from .handler import worker as handler_worker

