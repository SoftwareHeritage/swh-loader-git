SWH-loader-core
===============

The Software Heritage Core Loader is a low-level loading utilities and
helpers used by other loaders.

The main entry points are classes:
- :class:`swh.loader.core.loader.BaseLoader` for loaders (e.g. svn)
- :class:`swh.loader.core.loader.DVCSLoader` for DVCS loaders (e.g. hg, git, ...)
- :class:`swh.loader.package.loader.PackageLoader` for Package loaders (e.g. PyPI, Npm, ...)
