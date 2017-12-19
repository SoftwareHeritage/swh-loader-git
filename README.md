SWH-loader-core
===============

The Software Heritage Directory Loader is a library module to try and
permit to start a new loader.

Actual loader modules which depends on it are:

- [swh-loader-debian](https://forge.softwareheritage.org/source/swh-loader-debian/)
- [swh-loader-dir](https://forge.softwareheritage.org/source/swh-loader-dir/)
- [swh-loader-git](https://forge.softwareheritage.org/source/swh-loader-git/)
- [swh-loader-mercurial](https://forge.softwareheritage.org/source/swh-loader-mercurial/)
- [swh-loader-svn](https://forge.softwareheritage.org/source/swh-loader-svn/)
- [swh-loader-tar](https://forge.softwareheritage.org/source/swh-loader-tar/)

The main entry points are classes:
- :class:`swh.loader.core.loader.SWHLoader` for stateful loaders
- :class:`swh.loader.core.loader.SWHStatelessLoader` for stateless loaders
