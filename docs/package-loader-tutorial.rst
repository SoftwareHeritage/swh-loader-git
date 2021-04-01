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
(remplace the names with what you think is relevant)::

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


Testing your loader
+++++++++++++++++++

TODO


Making your loader more efficient
---------------------------------

TODO
