swh-loader-git
==============

The Software Heritage Git Loader is a tool and a library to walk a local
Git repository and inject into the SWH dataset all contained files that
weren't known before.

The main entry points are:

- ``swh.loader.git.loader.GitLoader`` for the main loader which can ingest
  either local or remote git repository's contents. This is the main
  implementation deployed in production.

- ``swh.loader.git.from_disk.GitLoaderFromDisk`` which ingests only local
  git clone repository.

- ``swh.loader.git.loader.GitLoaderFromArchive`` which ingests a git
  repository wrapped in an archive.

- ``swh.loader.git.directory.GitCheckoutLoader`` which ingests a git tree
  at a specific commit, branch or tag.


License
-------

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

See top-level LICENSE file for the full text of the GNU General Public
License along with this program.

Dependencies
------------

### Runtime

-   python3
-   python3-dulwich
-   python3-swh.core
-   python3-swh.model
-   python3-swh.storage
-   python3-swh.scheduler
-   ``libssl3`` — runtime TLS (universally pre-installed; required because
    the gitoxide-backed fetch uses libcurl with OpenSSL).

### Build (for the gitoxide Rust extension ``_gix``)

-   Rust toolchain (``rustup``-installed stable or pinned via
    ``rust-toolchain.toml``).
-   ``maturin`` (build front-end for PyO3 extensions, ``pip install
    maturin``).
-   ``libssl-dev`` (Debian/Ubuntu) or ``openssl-devel`` (RPM) — needed
    for the curl-rust crate's ``ssl`` feature, which links against the
    system OpenSSL for TLS. The previous ``rustls`` backend was dropped
    because the curl crate's ``rustls`` feature does not wire a default
    cert verifier and HTTPS handshakes failed at runtime.
-   ``build-essential``, ``pkg-config``, ``cmake``, ``autoconf``
    (transitive build deps for the Rust crate and for ``swh.shard``'s
    cmph submodule).

### Test

The test suite runs with ``pytest`` and uses fixtures that require a
real PostgreSQL server plus editable installs of sibling SWH packages.

1. **Editable installs of sibling SWH packages.**  The ``swh-environment``
   layout uses ``mr`` to keep all SWH repos checked out side by side;
   each one must be ``pip install -e``'d in the same venv as
   ``swh-loader-git`` so the imports resolve to the local trees.

   The ``swh-loader-core`` shared test helpers
   (``swh.loader.tests.__init__``) import ``swh.vault.to_disk.DirectoryBuilder``
   at module load time, so a missing ``swh-vault`` install makes every
   test file in ``swh-loader-git`` fail to *collect*::

      pip install -e ../swh-vault

   In general, run ``pip install -e .`` in every ``../swh-*/`` checkout
   you want active.

2. **PostgreSQL 17 system install.**  ``pytest-postgresql`` boots a
   temporary cluster against ``/usr/lib/postgresql/17/bin/pg_ctl``.
   On Debian 13 / Ubuntu 24.04+::

      sudo apt install postgresql-17

   On distros that ship a different PG version, override the executor
   path via the ``--postgresql-exec`` pytest flag or the
   ``POSTGRESQL_EXEC`` environment variable; see the
   ``pytest-postgresql`` docs for details.

3. **Python test deps.**  Installed via the package's
   ``requirements-test.txt``::

      pip install -r requirements-test.txt

Once those are in place, run the suite from the package root::

   make test

Failures of the form ``ModuleNotFoundError: No module named 'swh.vault'``
at collection time mean step 1 is missing; failures of the form
``ExecutableMissingException: Could not found /usr/lib/postgresql/17/bin/pg_ctl``
during fixture setup mean step 2 is missing.

Requirements
------------

-   implementation language, Python3
-   coding guidelines: conform to PEP8
-   Git access: via dulwich

CLI Run
----------

You can run the loader from a remote origin (*loader*) or from an origin on
disk (*from_disk*) directly by calling:

.. code-block:: shell

   swh loader -C <config-file> run git <git-repository-url>

or "git_disk".

## Configuration sample

/tmp/git.yml:

.. code-block:: yaml

   storage:
     cls: remote
     args:
       url: http://localhost:5002/
