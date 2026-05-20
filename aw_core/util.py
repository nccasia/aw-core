import sys
from typing import Tuple
import logging
import random

logger = logging.getLogger(__name__)


class VersionException(Exception):
    ...


def _version_info_tuple() -> Tuple[int, int, int]:  # pragma: no cover
    return (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)


def assert_version(required_version: Tuple[int, ...] = (3, 5)):  # pragma: no cover
    actual_version = _version_info_tuple()
    if actual_version <= required_version:
        raise VersionException(
            (
                "Python version {} not supported, you need to upgrade your Python"
                + " version to at least {}."
            ).format(required_version)
        )
    logger.debug(f"Python version: {_version_info_tuple()}")

def random_string(length: int) -> str:
    STRING_POOL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(STRING_POOL) for _ in range(length))