swh-loader-git
==============

The Software Heritage Git Loader is a tool and a library to walk a local
Git repository and inject into the SWH dataset all contained files that
weren't known before.

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

Configuration
-------------

You can run the loader or the updater directly by calling:
```
python3 -m swh.loader.git.{loader,updater}
```

### Location

Both tools expect a configuration file.

Either one of the following location:
- /etc/softwareheritage/
- ~/.config/swh/
- ~/.swh/

Note: Will call that location $SWH_CONFIG_PATH

### Configuration sample

$SWH_CONFIG_PATH/loader/git-{loader,updater}.yml:
```
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```
