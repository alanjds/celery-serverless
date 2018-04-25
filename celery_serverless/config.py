#coding: utf-8
from __future__ import unicode_literals, absolute_import
import os
import functools
from pathlib import Path

from ruamel.yaml import YAML
yaml = YAML()


@functools.lru_cache(1)
def get_config(file_path='./serverless.yml'):
    if not os.path.exists(file_path):
        raise RuntimeError(f"No file '{file_path}' detected."
                           " Please run 'celery serverless init' to create one if not exists")
    return yaml.load(Path(file_path))
