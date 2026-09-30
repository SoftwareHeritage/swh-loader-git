# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Remote logging utilities
"""

import logging
import time
from typing import List, Tuple

from .utils import LOGGING_INTERVAL


def split_lines_and_remainder(buf: bytes) -> Tuple[List[bytes], bytes]:
    """Get newline-terminated (``b"\\r"`` or ``b"\\n"``) lines from `buf`,
    and the beginning of the last line if it isn't terminated."""

    lines = buf.splitlines(keepends=True)
    if not lines:
        return [], b""

    if buf.endswith((b"\r", b"\n")):
        # The buffer ended with a newline, everything can be sent as lines
        return lines, b""
    else:
        # The buffer didn't end with a newline, we need to keep the
        # last bit as the beginning of the next line
        return lines[:-1], lines[-1]


class RemoteLogger:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

        self.remote_lines_elided = 0
        """Number of lines (ending with a carriage return) elided when debug
        logging is not enabled
        """

        self.last_elision_logged = time.monotonic()
        """Timestamp where the last elision was logged"""

        self.next_line_buf = b""
        """
        This buffer keeps the end of what do_remote has received, across
        calls, if it happens to be unterminated
        """

    def _maybe_log_elision(self, force: bool = False):
        if self.remote_lines_elided and (
            force
            # Always log at least every LOGGING_INTERVAL
            or time.monotonic() > self.last_elision_logged + LOGGING_INTERVAL
        ):
            self.logger.info(
                "%s remote line%s elided",
                self.remote_lines_elided,
                "s" if self.remote_lines_elided > 1 else "",
            )
            self.remote_lines_elided = 0
            self.last_elision_logged = time.monotonic()

    def _log_remote_message(self, line: bytes):
        do_debug = self.logger.isEnabledFor(logging.DEBUG)

        if not line.endswith(b"\n"):
            # This is a verbose line, ending with a carriage return only
            if do_debug:
                if stripped := line.strip():
                    self.logger.debug(
                        "remote: %s", stripped.decode("utf-8", "backslashreplace")
                    )
            else:
                self.remote_lines_elided += 1
                self._maybe_log_elision()
        else:
            # This is the last line in the current section, we will always log it
            self._maybe_log_elision(force=True)
            if stripped := line.strip():
                self.logger.info(
                    "remote: %s", stripped.decode("utf-8", "backslashreplace")
                )

    def do_progress(self, msg: bytes) -> None:
        lines, self.next_line_buf = split_lines_and_remainder(self.next_line_buf + msg)

        for line in lines:
            self._log_remote_message(line)

    def flush(self):
        # Always log what remains in the next_line_buf, if it's not empty
        self._maybe_log_elision(force=True)
        self._log_remote_message(self.next_line_buf)
