SWH-loader-core
===============

The Software Heritage Directory Loader is a library module to try and
permit to start a new loader.

Actual loader modules which depends on it are:

- [swh-loader-dir](https://forge.softwareheritage.org/source/swh-loader-dir/)
- [swh-loader-tar](https://forge.softwareheritage.org/source/swh-loader-tar/)
- [swh-loader-svn](https://forge.softwareheritage.org/source/swh-loader-svn/)

The main entry point is the class :class:`swh.loader.core.loader.SWHLoader`.
