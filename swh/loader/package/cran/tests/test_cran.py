# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest

from datetime import datetime, timezone
from dateutil.tz import tzlocal

from os import path

from swh.loader.package.cran.loader import (
    extract_intrinsic_metadata,
    CRANLoader,
    parse_date,
    parse_debian_control,
)
from swh.core.tarball import uncompress
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Snapshot, SnapshotBranch, TargetType, TimestampWithTimezone

from swh.loader.tests import (
    assert_last_visit_matches,
    check_snapshot,
    get_stats,
)


SNAPSHOT = Snapshot(
    id=hash_to_bytes("920adcccc78aaeedd3cfa4459dd900d8c3431a21"),
    branches={
        b"HEAD": SnapshotBranch(
            target=b"releases/2.22-6", target_type=TargetType.ALIAS
        ),
        b"releases/2.22-6": SnapshotBranch(
            target=hash_to_bytes("42bdb16facd5140424359c8ce89a28ecfa1ce603"),
            target_type=TargetType.REVISION,
        ),
    },
)


def test_cran_parse_date():
    data = [
        # parsable, some have debatable results though
        ("2001-June-08", datetime(2001, 6, 8, 0, 0, tzinfo=timezone.utc)),
        (
            "Tue Dec 27 15:06:08 PST 2011",
            datetime(2011, 12, 27, 15, 6, 8, tzinfo=timezone.utc),
        ),
        ("8-14-2013", datetime(2013, 8, 14, 0, 0, tzinfo=timezone.utc)),
        ("2011-01", datetime(2011, 1, 1, 0, 0, tzinfo=timezone.utc)),
        ("201109", datetime(2009, 11, 20, 0, 0, tzinfo=timezone.utc)),
        ("04-12-2014", datetime(2014, 4, 12, 0, 0, tzinfo=timezone.utc)),
        (
            "2018-08-24, 10:40:10",
            datetime(2018, 8, 24, 10, 40, 10, tzinfo=timezone.utc),
        ),
        ("2013-October-16", datetime(2013, 10, 16, 0, 0, tzinfo=timezone.utc)),
        ("Aug 23, 2013", datetime(2013, 8, 23, 0, 0, tzinfo=timezone.utc)),
        ("27-11-2014", datetime(2014, 11, 27, 0, 0, tzinfo=timezone.utc)),
        ("2019-09-26,", datetime(2019, 9, 26, 0, 0, tzinfo=timezone.utc)),
        ("9/25/2014", datetime(2014, 9, 25, 0, 0, tzinfo=timezone.utc)),
        (
            "Fri Jun 27 17:23:53 2014",
            datetime(2014, 6, 27, 17, 23, 53, tzinfo=timezone.utc),
        ),
        ("28-04-2014", datetime(2014, 4, 28, 0, 0, tzinfo=timezone.utc)),
        ("04-14-2014", datetime(2014, 4, 14, 0, 0, tzinfo=timezone.utc)),
        (
            "2019-05-08 14:17:31 UTC",
            datetime(2019, 5, 8, 14, 17, 31, tzinfo=timezone.utc),
        ),
        (
            "Wed May 21 13:50:39 CEST 2014",
            datetime(2014, 5, 21, 13, 50, 39, tzinfo=tzlocal()),
        ),
        (
            "2018-04-10 00:01:04 KST",
            datetime(2018, 4, 10, 0, 1, 4, tzinfo=timezone.utc),
        ),
        ("2019-08-25 10:45", datetime(2019, 8, 25, 10, 45, tzinfo=timezone.utc)),
        ("March 9, 2015", datetime(2015, 3, 9, 0, 0, tzinfo=timezone.utc)),
        ("Aug. 18, 2012", datetime(2012, 8, 18, 0, 0, tzinfo=timezone.utc)),
        ("2014-Dec-17", datetime(2014, 12, 17, 0, 0, tzinfo=timezone.utc)),
        ("March 01, 2013", datetime(2013, 3, 1, 0, 0, tzinfo=timezone.utc)),
        ("2017-04-08.", datetime(2017, 4, 8, 0, 0, tzinfo=timezone.utc)),
        ("2014-Apr-22", datetime(2014, 4, 22, 0, 0, tzinfo=timezone.utc)),
        (
            "Mon Jan 12 19:54:04 2015",
            datetime(2015, 1, 12, 19, 54, 4, tzinfo=timezone.utc),
        ),
        ("May 22, 2014", datetime(2014, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (
            "2014-08-12 09:55:10 EDT",
            datetime(2014, 8, 12, 9, 55, 10, tzinfo=timezone.utc),
        ),
        # unparsable
        ("Fabruary 21, 2012", None),
        ('2019-05-28"', None),
        ("2017-03-01 today", None),
        ("2016-11-0110.1093/icesjms/fsw182", None),
        ("2019-07-010", None),
        ("2015-02.23", None),
        ("20013-12-30", None),
        ("2016-08-017", None),
        ("2019-02-07l", None),
        ("2018-05-010", None),
        ("2019-09-27 KST", None),
        ("$Date$", None),
        ("2019-09-27 KST", None),
        ("2019-06-22 $Date$", None),
        ("$Date: 2013-01-18 12:49:03 -0600 (Fri, 18 Jan 2013) $", None),
        ("2015-7-013", None),
        ("2018-05-023", None),
        ("Check NEWS file for changes: news(package='simSummary')", None),
    ]
    for date, expected_date in data:
        actual_tstz = parse_date(date)
        if expected_date is None:
            assert actual_tstz is None, date
        else:
            expected_tstz = TimestampWithTimezone.from_datetime(expected_date)
            assert actual_tstz == expected_tstz, date


@pytest.mark.fs
def test_extract_intrinsic_metadata(tmp_path, datadir):
    """Parsing existing archive's PKG-INFO should yield results"""
    uncompressed_archive_path = str(tmp_path)
    # sample url
    # https://cran.r-project.org/src_contrib_1.4.0_Recommended_KernSmooth_2.22-6.tar.gz  # noqa
    archive_path = path.join(
        datadir,
        "https_cran.r-project.org",
        "src_contrib_1.4.0_Recommended_KernSmooth_2.22-6.tar.gz",
    )
    uncompress(archive_path, dest=uncompressed_archive_path)

    actual_metadata = extract_intrinsic_metadata(uncompressed_archive_path)

    expected_metadata = {
        "Package": "KernSmooth",
        "Priority": "recommended",
        "Version": "2.22-6",
        "Date": "2001-June-08",
        "Title": "Functions for kernel smoothing for Wand & Jones (1995)",
        "Author": "S original by Matt Wand.\n\tR port by  Brian Ripley <ripley@stats.ox.ac.uk>.",  # noqa
        "Maintainer": "Brian Ripley <ripley@stats.ox.ac.uk>",
        "Description": 'functions for kernel smoothing (and density estimation)\n  corresponding to the book: \n  Wand, M.P. and Jones, M.C. (1995) "Kernel Smoothing".',  # noqa
        "License": "Unlimited use and distribution (see LICENCE).",
        "URL": "http://www.biostat.harvard.edu/~mwand",
    }

    assert actual_metadata == expected_metadata


@pytest.mark.fs
def test_extract_intrinsic_metadata_failures(tmp_path):
    """Parsing inexistent path/archive/PKG-INFO yield None"""
    # inexistent first level path
    assert extract_intrinsic_metadata("/something-inexistent") == {}
    # inexistent second level path (as expected by pypi archives)
    assert extract_intrinsic_metadata(tmp_path) == {}
    # inexistent PKG-INFO within second level path
    existing_path_no_pkginfo = str(tmp_path / "something")
    os.mkdir(existing_path_no_pkginfo)
    assert extract_intrinsic_metadata(tmp_path) == {}


def test_cran_one_visit(swh_config, requests_mock_datadir):
    version = "2.22-6"
    base_url = "https://cran.r-project.org"
    origin_url = f"{base_url}/Packages/Recommended_KernSmooth/index.html"
    artifact_url = (
        f"{base_url}/src_contrib_1.4.0_Recommended_KernSmooth_{version}.tar.gz"  # noqa
    )
    loader = CRANLoader(
        origin_url, artifacts=[{"url": artifact_url, "version": version,}]
    )

    actual_load_status = loader.load()

    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": SNAPSHOT.id.hex(),
    }

    check_snapshot(SNAPSHOT, loader.storage)

    assert_last_visit_matches(loader.storage, origin_url, status="full", type="cran")

    visit_stats = get_stats(loader.storage)
    assert {
        "content": 33,
        "directory": 7,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    } == visit_stats

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith(base_url)
    ]
    # visited each artifact once across 2 visits
    assert len(urls) == 1


def test_cran_2_visits_same_origin(swh_config, requests_mock_datadir):
    """Multiple visits on the same origin, only 1 archive fetch"""
    version = "2.22-6"
    base_url = "https://cran.r-project.org"
    origin_url = f"{base_url}/Packages/Recommended_KernSmooth/index.html"
    artifact_url = (
        f"{base_url}/src_contrib_1.4.0_Recommended_KernSmooth_{version}.tar.gz"  # noqa
    )
    loader = CRANLoader(
        origin_url, artifacts=[{"url": artifact_url, "version": version}]
    )

    # first visit
    actual_load_status = loader.load()

    expected_snapshot_id = "920adcccc78aaeedd3cfa4459dd900d8c3431a21"
    assert actual_load_status == {
        "status": "eventful",
        "snapshot_id": SNAPSHOT.id.hex(),
    }

    check_snapshot(SNAPSHOT, loader.storage)

    assert_last_visit_matches(loader.storage, origin_url, status="full", type="cran")

    visit_stats = get_stats(loader.storage)
    assert {
        "content": 33,
        "directory": 7,
        "origin": 1,
        "origin_visit": 1,
        "person": 1,
        "release": 0,
        "revision": 1,
        "skipped_content": 0,
        "snapshot": 1,
    } == visit_stats

    # second visit
    actual_load_status2 = loader.load()

    assert actual_load_status2 == {
        "status": "uneventful",
        "snapshot_id": expected_snapshot_id,
    }

    assert_last_visit_matches(loader.storage, origin_url, status="full", type="cran")

    visit_stats2 = get_stats(loader.storage)
    visit_stats["origin_visit"] += 1
    assert visit_stats2 == visit_stats, "same stats as 1st visit, +1 visit"

    urls = [
        m.url
        for m in requests_mock_datadir.request_history
        if m.url.startswith(base_url)
    ]
    assert len(urls) == 1, "visited one time artifact url (across 2 visits)"


def test_parse_debian_control(datadir):
    description_file = os.path.join(datadir, "description", "acepack")

    actual_metadata = parse_debian_control(description_file)

    assert actual_metadata == {
        "Package": "acepack",
        "Maintainer": "Shawn Garbett",
        "Version": "1.4.1",
        "Author": "Phil Spector, Jerome Friedman, Robert Tibshirani...",
        "Description": "Two nonparametric methods for multiple regression...",
        "Title": "ACE & AVAS 4 Selecting Multiple Regression Transformations",
        "License": "MIT + file LICENSE",
        "Suggests": "testthat",
        "Packaged": "2016-10-28 15:38:59 UTC; garbetsp",
        "Repository": "CRAN",
        "Date/Publication": "2016-10-29 00:11:52",
        "NeedsCompilation": "yes",
    }


def test_parse_debian_control_unicode_issue(datadir):
    # iso-8859-1 caused failure, now fixed
    description_file = os.path.join(datadir, "description", "KnownBR")

    actual_metadata = parse_debian_control(description_file)

    assert actual_metadata == {
        "Package": "KnowBR",
        "Version": "2.0",
        "Title": """Discriminating Well Surveyed Spatial Units from Exhaustive
        Biodiversity Databases""",
        "Author": "Cástor Guisande González and Jorge M. Lobo",
        "Maintainer": "Cástor Guisande González <castor@email.es>",
        "Description": "It uses species accumulation curves and diverse estimators...",
        "License": "GPL (>= 2)",
        "Encoding": "latin1",
        "Depends": "R (>= 3.0), fossil, mgcv, plotrix, sp, vegan",
        "Suggests": "raster, rgbif",
        "NeedsCompilation": "no",
        "Packaged": "2019-01-30 13:27:29 UTC; castor",
        "Repository": "CRAN",
        "Date/Publication": "2019-01-31 20:53:50 UTC",
    }
