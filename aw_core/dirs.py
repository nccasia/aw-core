import os
from functools import wraps
from typing import Optional, Callable
from aw_core.util import random_string

import appdirs

GetDirFunc = Callable[[Optional[str]], str]


def ensure_path_exists(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def _ensure_returned_path_exists(f: GetDirFunc) -> GetDirFunc:
    @wraps(f)
    def wrapper(subpath: Optional[str]) -> str:
        path = f(subpath)
        ensure_path_exists(path)
        return path

    return wrapper


@_ensure_returned_path_exists
def get_data_dir(module_name: Optional[str]) -> str:
    data_dir = appdirs.user_data_dir("komutracker")
    return os.path.join(data_dir, module_name) if module_name else data_dir


@_ensure_returned_path_exists
def get_cache_dir(module_name: Optional[str]) -> str:
    cache_dir = appdirs.user_cache_dir("komutracker")
    return os.path.join(cache_dir, module_name) if module_name else cache_dir


@_ensure_returned_path_exists
def get_config_dir(module_name: Optional[str]) -> str:
    config_dir = appdirs.user_config_dir("komutracker")
    return os.path.join(config_dir, module_name) if module_name else config_dir


@_ensure_returned_path_exists
def get_log_dir(module_name: Optional[str]) -> str:  # pragma: no cover
    log_dir = appdirs.user_log_dir("komutracker")
    return os.path.join(log_dir, module_name) if module_name else log_dir


def get_device_id() -> str:
    """Return a persistent random device ID, creating it on first call.

    The ID is stored as a 64-character hex string in a hidden file
    inside the komutracker data directory.
    """
    data_dir = get_data_dir(None)
    device_id_path = os.path.join(data_dir, ".device_id")
    if os.path.exists(device_id_path):
        with open(device_id_path, "r") as f:
            return f.read().strip()
    device_id = random_string(64)
    with open(device_id_path, "w") as f:
        f.write(device_id)
    return device_id
