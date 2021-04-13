Software Heritage - Loader foundations
======================================

The Software Heritage Loader Core is a low-level loading utilities and
helpers used by :term:`loaders <loader>`.

The main entry points are classes:
- :class:`swh.loader.core.loader.BaseLoader` for loaders (e.g. svn)
- :class:`swh.loader.core.loader.DVCSLoader` for DVCS loaders (e.g. hg, git, ...)
- :class:`swh.loader.package.loader.PackageLoader` for Package loaders (e.g. PyPI, Npm, ...)

Package loaders
---------------

This package also implements many package loaders directly, out of convenience,
as they usually are quite similar and each fits in a single file.

They all roughly follow these steps, explained in the
:py:meth:`swh.loader.package.loader.PackageLoader.load` documentation.
See the :ref:`package-loader-tutorial` for details.

VCS loaders
-----------

Unlike package loaders, VCS loaders remain in separate packages,
as they often need more advanced conversions and very VCS-specific operations.

This usually involves getting the branches of a repository and recursively loading
revisions in the history (and directory trees in these revisions),
until a known revision is found
