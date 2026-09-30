# Copyright (C) 2018-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.loader.git.remote_logs import split_lines_and_remainder


@pytest.mark.parametrize(
    "input,output",
    (
        (b"", ([], b"")),
        (b"trailing", ([], b"trailing")),
        (b"line1\r", ([b"line1\r"], b"")),
        (b"line1\rtrailing", ([b"line1\r"], b"trailing")),
        (b"line1\r\ntrailing", ([b"line1\r\n"], b"trailing")),
        (b"line1\r\nline2\ntrailing", ([b"line1\r\n", b"line2\n"], b"trailing")),
        (b"line1\r\nline2\nline3\r", ([b"line1\r\n", b"line2\n", b"line3\r"], b"")),
    ),
)
def test_split_lines_and_remainder(input, output):
    assert split_lines_and_remainder(input) == output
