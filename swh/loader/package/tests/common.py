# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from os import path
from typing import Dict, List, Tuple

logger = logging.getLogger(__file__)


DATADIR = path.join(path.abspath(path.dirname(__file__)), "resources")


def check_metadata(metadata: Dict, key_path: str, raw_type: str):
    """Given a metadata dict, ensure the associated key_path value is of type
       raw_type.

    Args:
        metadata: Dict to check
        key_path: Path to check
        raw_type: Type to check the path with

    Raises:
        Assertion error in case of mismatch

    """
    data = metadata
    keys = key_path.split(".")
    for k in keys:
        try:
            data = data[k]
        except (TypeError, KeyError) as e:
            # KeyError: because path too long
            # TypeError: data is not a dict
            raise AssertionError(e)
    assert isinstance(data, raw_type)  # type: ignore


def check_metadata_paths(metadata: Dict, paths: List[Tuple[str, str]]):
    """Given a metadata dict, ensure the keys are of expected types

    Args:
        metadata: Dict to check
        key_path: Path to check
        raw_type: Type to check the path with

    Raises:
        Assertion error in case of mismatch

    """
    for key_path, raw_type in paths:
        check_metadata(metadata, key_path, raw_type)
