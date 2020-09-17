# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert objects to dictionaries suitable for swh.storage"""

import logging
from typing import Dict, Iterable, List, Optional, Tuple

from swh.model.hashutil import hash_to_hex
from swh.model.model import BaseContent, Content, SkippedContent

logger = logging.getLogger(__name__)


def prepare_contents(
    contents: Iterable[Dict],
    max_content_size: Optional[int] = None,
    origin_url: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """Prepare contents for storage from a list of contents

    Returns
        tuple of content iterable, skipped content iterable

    """
    present_contents: List[Dict] = []
    skipped_contents: List[Dict] = []
    for _content in contents:
        content = content_for_storage(
            _content, max_content_size=max_content_size, origin_url=origin_url
        )
        if isinstance(content, SkippedContent):
            skipped_contents.append(content.to_dict())
        else:
            present_contents.append(content.to_dict())
    return present_contents, skipped_contents


def content_for_storage(
    content: Dict,
    max_content_size: Optional[int] = None,
    origin_url: Optional[str] = None,
) -> BaseContent:
    """Prepare content to be ready for storage

    Note:
    - 'data' is returned only if max_content_size is not reached.

    Returns:
        content with added data (or reason for being missing)

    """
    ret = content.copy()
    ret.pop("perms", None)

    if max_content_size and ret["length"] > max_content_size:
        logger.info(
            "Skipping content %s, too large (%s > %s)"
            % (hash_to_hex(content["sha1_git"]), ret["length"], max_content_size)
        )
        ret.pop("data", None)
        ret.update(
            {"status": "absent", "reason": "Content too large", "origin": origin_url}
        )
        return SkippedContent.from_dict(ret)

    if "data" not in ret:
        with open(ret["path"], "rb") as f:
            ret["data"] = f.read()

    # Extra keys added by swh.model.from_disk, that are not accepted
    # by swh-storage
    ret.pop("path", None)

    ret["status"] = "visible"

    return Content.from_dict(ret)
