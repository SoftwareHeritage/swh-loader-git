.. _package-loader-tutorial:

Package Loader Tutorial
=======================

In this tutorial, we will see how to write a loader for |swh| that loads
packages from a package manager, such as PyPI or Debian's.

First, you should be familiar with Python, unit-testing,
|swh|'s :ref:`data-model` and :ref:`architecture`,
and go through the :ref:`developer-setup`.


Creating the files hierarchy
----------------------------

Once this is done, you should create a new directory (ie. a (sub)package from
Python's point of view) for you loader.
It can be either a subdirectory of ``swh-loader-core/swh/loader/package/`` like
the other package loaders, or it can be in its own package.

If you choose the latter, you should also create the base file of any Python
package (such as ``setup.py``), you should import them from the `swh-py-template`_
repository.

In the rest of this tutorial, we will assume you chose the former and
your loader is named "New Loader", so your package loader is in
``swh-loader-core/swh/loader/package/newloader/``.

Next, you should create boilerplate files needed for SWH loaders: ``__init__.py``,
``tasks.py``, ``tests/__init__.py``, and ``tests/test_tasks.py``;
copy them from an existing package, such as
``swh-loader-core/swh/loader/package/pypi/``, and replace the names in those
with your loader's.

Finally, create an `entrypoint`_ in ``setup.py``, so your loader can be discovered
by the SWH Celery workers::

    entry_points="""
        [swh.workers]
        loader.newloader=swh.loader.package.newloader:register
    """,

.. _swh-py-template: https://forge.softwareheritage.org/source/swh-py-template/
.. _entrypoint: https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html


Writing a minimal loader
------------------------

It is now time for the interesting part: writing the code to load packages from
a package manager into the |swh| archive.

Create a file named ``loader.py`` in your package's directory, with two empty classes
(replace the names with what you think is relevant)::

   from typing import Optional

   import attr

   from swh.loader.package.loader import BasePackageInfo, PackageLoader
   from swh.model.model import Person, Revision, Sha1Git, TimestampWithTimezone


   @attr.s
   class NewPackageInfo(BasePackageInfo):
       pass

   class NewLoader(PackageLoader[NewPackageInfo]):
       visit_type = "newloader"


We now have to fill some of the methods declared by
:ref:class:`swh.loader.package.PackageLoader`: in your new ``NewLoader`` class.


Listing versions
++++++++++++++++

``get_versions`` should return the list of names of all versions of the origin
defined at ``self.url`` by the default constructor; and ``get_default_version``
should return the name of the default version (usually the latest stable release).

They are both implemented with an API call to the package repository.
For example, for PyPI origin https://pypi.org/project/requests, this is done
with a request to https://pypi.org/pypi/requests/json.


Getting package information
+++++++++++++++++++++++++++

Next, ``get_package_info`` takes as argument a version name
(as returned by ``get_versions``) and yields ``(branch_name, p_info)`` tuples,
where ``branch_name`` is a string and ``pkg_info`` is an instance
of the ``NewPackageInfo`` class we defined earlier.

Each of these tuples should match a single file the loader will download
from the origin. Usually, there is only one file per versions, but this is not
true for all package repositories (eg. CRAN and PyPI allow multiple version artifacts
per version).

As ``NewPackageInfo`` derives from :py:class:`swh.loader.package.BasePackageInfo`,
it can be created like this::

   return NewPackageInfo(url="https://...", filename="...-versionX.Y.tar.gz")

The ``url`` must be a URL where to download the archive from.
``filename`` is optional, but it is nice to fill it when possible/relevant.

The base ``PackageLoader`` will then take care of calling ``get_versions()``
to get all the versions, then call ``get_package_info()`` get the list
of archives to download, download them, and load all the directories in the archive.

This means you do not need to manage downloads yourself; and we are now done with
interactions with the package repository.


Building a revision
+++++++++++++++++++

The final step for your minimal loader to work, is to implement ``build_revision``.
This is a very important part, as it will create a revision object that will be
inserted in |swh|, as a link between origins and the directories.

This function takes three important arguments:

* ``p_info`` is an object returned by ``get_package_info()``
* ``uncompressed_path`` is the location on the disk where the base ``PackageLoader``
  extracted the archive, so you can access files from the archive.
* ``directory`` is an :term:`intrinsic identifier` of the directory that was loaded
  from the archive

The way to implement it depends very much on how the package manager works,
but here is a rough idea::

    def build_revision(
        self, p_info: NewPackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        author = Person(name="Jane Doe", email="jdoe@example.org")
        date = TimestampWithTimezone.from_iso8601("2021-04-01T11:55:20Z")

        return Revision(
            type=RevisionType.TAR,
            message="This is a new release of the project",
            author=author,
            date=date,
            committer=author,
            committer_date=date,
            parents=(),
            directory=directory,
            synthetic=True,
        )

The strings here are placeholders, and you should extract them from either
the extracted archive (using ``uncompressed_path``), or from the package repository's
API.
The various classes used in this example are :py:class:`swh.model.model.Person`,
:py:class:`swh.model.model.TimestampWithTimezone`,
and :py:class:`swh.model.model.Revision`.

Note that you have access to the ``NewPackageInfo`` object created by
``get_package_info()``, so you can extend the ``NewPackageInfo`` class to pass
data between these two functions.

A few caveats:

* Make sure the timezone matches the source's
* ``Person`` can also be built with just a ``fullname``, if there aren't distinct
  fields for name and email. When in doubt, it's better to just write the ``fullname``
  than try to parse it
* ``author`` and ``committer`` (resp. ``date`` and ``committer_date``) may be different
  if the release was written and published by different people (resp. dates).
  This is only relevant when loading from VCS, so you can usually ignore it
  in you package loader.


Running your loader
+++++++++++++++++++

With Docker
^^^^^^^^^^^

We recommend you use our `Docker environment`_ to test your loader.

In short, install Docker, ``cd`` to ``swh-environment/docker/``,
then `edit docker-compose.override.yml`_ to insert your new loader in the Docker
environment, something like this will do::

   version: '2'

   services:
     swh-loader-core:
       volumes:
         - "$HOME/swh-environment/swh-loader-core:/src/swh-loader-core"

Then start the Docker environment::

   docker-compose start

Then, you can run your loader::

   docker-compose exec swh-loader swh loader run newloader "https://example.org/~jdoe/project/"

where ``newloader`` is the name you registered as an entrypoint in ``setup.py`` and
``https://example.org/~jdoe/project/`` is the origin URL, that will be set as the
``self.url`` attribute of your loader.


For example, to run the PyPI loader, the command would be::

   docker-compose exec swh-loader swh loader run pypi "https://pypi.org/project/requests/"


If you get this error, make sure you properly configured
``docker-compose.override.yml``::

   Error: Invalid value for '[...]': invalid choice: newloader


Without Docker
^^^^^^^^^^^^^^

If you do not want to use the Docker environment, you will need to start
an :ref:`swh-storage` instance yourself, and create a config file that references it::

   storage:
     cls: remote
     url: http://localhost:5002/

Or alternatively, this more efficient configuration::

   storage:
     cls: pipeline
     steps:
       - cls: buffer
         min_batch_size:
           content: 10000
           content_bytes: 104857600
           directory: 1000
           revision: 1000
       - cls: filter
       - cls: remote
         url: http://localhost:5002/

And run your loader with::

   swh loader -C loader.yml run newloader "https://example.org/~jdoe/project/"

where ``newloader`` is the name you registered as an entrypoint in ``setup.py`` and
``https://example.org/~jdoe/project/`` is the origin URL, that will be set as the
``self.url`` attribute of your loader.

For example, with PyPI::

   swh loader -C loader.yml run pypi "https://pypi.org/project/requests/"


.. _Docker environment: https://forge.softwareheritage.org/source/swh-environment/browse/master/docker/
.. _edit docker-compose.override.yml: https://forge.softwareheritage.org/source/swh-environment/browse/master/docker/#install-a-swh-package-from


Testing your loader
+++++++++++++++++++

You must write tests for your loader.

First, of course, unit tests for the internal functions of your loader, if any
(eg. the functions used to extract metadata); but this is not covered in this tutorial.

Most importantly, you should write integration tests for your loader,
that will simulate an origin, run the loader, and check everything is loaded
in the storage as it should be.

As we do not want tests to directly query an origin (it makes tests flaky, hard to
reproduce, and put unnecessary load on the origin), we usually mock it using
the :py:func:`swh.core.pytest_plugin.requests_mock_datadir` fixture

It works by creating a ``data/`` folder in your tests (such as
``swh/loader/package/newloader/tests/data/``) and downloading results from API
calls there, in the structured documented in
:py:func:`swh.core.pytest_plugin.requests_mock_datadir_factory`

The files in the ``datadir/`` will then be served whenever the loader tries to access
an URL. This is very dependent on the kind of repositories your loader will read from,
so here is an example with the PyPI loader.

The files
``swh/loader/package/pypi/tests/data/https_pypi.org/pypi_nexter_json`` and
``swh/loader/package/pypi/tests/data/https_files.pythonhosted.org/nexter-*``
are used in this test::

   from swh.loader.tests import assert_last_visit_matches, check_snapshot, get_stats

   def test_pypi_visit_1_release_with_2_artifacts(swh_storage, requests_mock_datadir):
       # Initialize the loader
       url = "https://pypi.org/project/nexter"
       loader = PyPILoader(swh_storage, url)

       # Run the loader, with a swh-storage instance, on the given URL.
       # HTTP calls will be mocked by the requests_mock_datadir fixture
       actual_load_status = loader.load()

       # Check the loader loaded exactly the snapshot we expected
       # (when writing your tests for the first time, you cannot know the
       # snapshot id without running your loader; so let it error and write
       # down the result here)
       expected_snapshot_id = hash_to_bytes("a27e638a4dad6fbfa273c6ebec1c4bf320fb84c6")
       assert actual_load_status == {
           "status": "eventful",
           "snapshot_id": expected_snapshot_id.hex(),
       }

       # Check the content of the snapshot. (ditto)
       expected_snapshot = Snapshot(
           id=expected_snapshot_id,
           branches={
               b"releases/1.1.0/nexter-1.1.0.zip": SnapshotBranch(
                   target=hash_to_bytes("4c99891f93b81450385777235a37b5e966dd1571"),
                   target_type=TargetType.REVISION,
               ),
               b"releases/1.1.0/nexter-1.1.0.tar.gz": SnapshotBranch(
                   target=hash_to_bytes("0bf88f5760cca7665d0af4d6575d9301134fe11a"),
                   target_type=TargetType.REVISION,
               ),
           },
       )
       check_snapshot(expected_snapshot, swh_storage)

       # Check the visit was properly created with the right type
       assert_last_visit_matches(
           swh_storage, url, status="full", type="pypi", snapshot=expected_snapshot.id
       )

       # Then you could check the directory structure:
       directory_id = swh_storage.revision_get(
          [hash_to_bytes("4c99891f93b81450385777235a37b5e966dd1571")]
       )[0].directory
       entries = list(swh_storage.directory_ls(directory_id, recursive=True))
       assert entries == [
           ...
       ]


Here are some scenarios you should test, when relevant:

* No versions
* One version
* Two or more versions
* More than one package per version, if relevant
* Corrupt packages (missing metadata, ...), if relevant
* API errors
* etc.


Making your loader more efficient
---------------------------------

TODO


Loading metadata
----------------

TODO
