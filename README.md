swh-loader-git
==============

The Software Heritage Git Loader is a tool and a library to walk a local
Git repository and inject into the SWH dataset all contained files that
weren't known before.

The main entry points are

- :class:`swh.loader.git.loader.GitLoader` for the main loader which ingests a remote git
  repository's contents.

- :class:`swh.loader.git.from_disk.GitLoaderFromDisk` which ingests a local git clone
  repository.

- :class:`swh.loader.git.loader.GitLoaderFromArchive` which ingests a git repository
  wrapped in an archive.


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
-   python3-retrying
-   python3-swh.core
-   python3-swh.model
-   python3-swh.storage
-   python3-swh.scheduler

### Test

-   python3-nose

Requirements
------------

-   implementation language, Python3
-   coding guidelines: conform to PEP8
-   Git access: via dulwich

CLI Run
----------

You can run the loader from a remote origin (*loader*) or from an origin on disk
(*from_disk*) directly by calling:

```
swh loader -C <config-file> run git <git-repository-url>
```

or "git_disk".

## Configuration sample

/tmp/git.yml:
```
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```
