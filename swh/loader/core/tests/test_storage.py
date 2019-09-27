# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.loader.package.storage import (
    BufferingProxyStorage, FilteringProxyStorage
)


sample_content = {
    'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
    'sha1': b'g\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
    'sha1_git': b'\xf2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
    'sha256': b"\x87\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
    'length': 48,
    'data': b'temp file for testing content storage conversion',
    'status': 'visible',
}

sample_content2 = {
    'blake2s256': b'\xbf?\x05\xed\xc1U\xd2\xc5\x168Xm\x93\xde}f(HO@\xd0\xacn\x04\x1e\x9a\xb9\xfa\xbf\xcc\x08\xc7',  # noqa
    'sha1': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
    'sha1_git': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
    'sha256': b"\x77\x022\xedZN\x84\xe8za\xf8'(oA\xc9k\xb1\x80c\x80\xe7J\x06\xea\xd2\xd5\xbeB\x19\xb8\xce",  # noqa
    'length': 50,
    'data': b'temp file for testing content storage conversion 2',
    'status': 'visible',
}


sample_directory = {
    'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
    'entries': []
}


sample_person = {
    'name': b'John Doe',
    'email': b'john.doe@institute.org',
    'fullname': b'John Doe <john.doe@institute.org>'
}


sample_revision = {
    'id': b'f\x15y+\xcb][\\\n\xf28\xb2\x0c_P[\xc8\x89Hk',
    'message': b'something',
    'author': sample_person,
    'committer': sample_person,
    'date': 1567591673,
    'committer_date': 1567591673,
    'type': 'tar',
    'directory': b'\xc2\xae\xfa\xba\xfa\xa6B\x9b^\xf9Z\xf5\x14\x0cna\xb0\xef\x8b',  # noqa
    'synthetic': False,
    'metadata': {},
    'parents': [],
}


def test_buffering_proxy_storage_content_threshold_not_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'content': 10,
        }
    )
    s = storage.content_add([sample_content, sample_content2])
    assert s == {}

    s = storage.flush()
    assert s == {
        'content:add': 1 + 1,
        'content:add:bytes': 48 + 50,
        'skipped_content:add': 0
    }


def test_buffering_proxy_storage_content_threshold_nb_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'content': 1,
        }
    )

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': 48,
        'skipped_content:add': 0
    }
    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_content_threshold_bytes_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'content': 10,
            'content_bytes': 20,
        }
    )

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': 48,
        'skipped_content:add': 0
    }
    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_directory_threshold_not_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'directory': 10,
        }
    )
    s = storage.directory_add([sample_directory])
    assert s == {}

    s = storage.flush()
    assert s == {
        'directory:add': 1,
    }


def test_buffering_proxy_storage_directory_threshold_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'directory': 1,
        }
    )
    s = storage.directory_add([sample_directory])
    assert s == {
        'directory:add': 1,
    }

    s = storage.flush()
    assert s == {}


def test_buffering_proxy_storage_revision_threshold_not_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'revision': 10,
        }
    )
    s = storage.revision_add([sample_revision])
    assert s == {}

    s = storage.flush()
    assert s == {
        'revision:add': 1,
    }


def test_buffering_proxy_storage_revision_threshold_hit():
    storage = BufferingProxyStorage(
        storage={'cls': 'memory', 'args': {}},
        thresholds={
            'revision': 1,
        }
    )
    s = storage.revision_add([sample_revision])
    assert s == {
        'revision:add': 1,
    }

    s = storage.flush()
    assert s == {}


def test_filtering_proxy_storage_content():
    storage = FilteringProxyStorage(storage={'cls': 'memory', 'args': {}})

    content = next(storage.content_get([sample_content['sha1']]))
    assert not content

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 1,
        'content:add:bytes': 48,
        'skipped_content:add': 0
    }

    content = next(storage.content_get([sample_content['sha1']]))
    assert content is not None

    s = storage.content_add([sample_content])
    assert s == {
        'content:add': 0,
        'content:add:bytes': 0,
        'skipped_content:add': 0
    }


def test_filtering_proxy_storage_revision():
    storage = FilteringProxyStorage(storage={'cls': 'memory', 'args': {}})

    revision = next(storage.revision_get([sample_revision['id']]))
    assert not revision

    s = storage.revision_add([sample_revision])
    assert s == {
        'revision:add': 1,
    }

    revision = next(storage.revision_get([sample_revision['id']]))
    assert revision is not None

    s = storage.revision_add([sample_revision])
    assert s == {
        'revision:add': 0,
    }


def test_filtering_proxy_storage_directory():
    storage = FilteringProxyStorage(storage={'cls': 'memory', 'args': {}})

    directory = next(storage.directory_missing([sample_directory['id']]))
    assert directory

    s = storage.directory_add([sample_directory])
    assert s == {
        'directory:add': 1,
    }

    directory = list(storage.directory_missing([sample_directory['id']]))
    assert not directory

    s = storage.directory_add([sample_directory])
    assert s == {
        'directory:add': 0,
    }
