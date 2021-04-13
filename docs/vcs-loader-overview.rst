.. _vcs-loader-tutorial:

VCS Loader Overview
===================

In this overview, we will see how to write a loader for |swh| that loads
:term:`artifacts <artifact>` from a Version Control System, such as Git,
Mercurial, or Subversion

First, you should be familiar with Python, unit-testing,
|swh|'s :ref:`data-model` and :ref:`architecture`,
and go through the :ref:`developer-setup`.

As seen in the :ref:`swh-loader-core homepage <swh-loader-core>`,
SWH loaders can be sorted into two large categories:
Package Loaders and VCS loaders.

This page is an overview of how to write a VCS loader. This is not a tutorial,
because VCS loaders are hooked deeply into their respective VCS' internals;
unlike :ref:`Package Loaders <package-loader-tutorial>` which are somewhat uniform
(list tarballs, download tarballs, load content of tarball, done).


Architecture
------------

A loader is a Python package, usually a subpackage of ``swh.loader``
but in its own directory (eg. ``swh-loader-git/swh/loader/git``, as ``swh.loader``
is a :pep:`namespace package <420>`), based on the `swh-py-template`_ repository.

It has at least one `entrypoint`_, declared in ``setup.py`` to be recognized
by ``swh-loader-core``::

    entry_points="""
        [swh.workers]
        loader.newloader=swh.loader.newloader:register
    """,

This entrypoint declares the task name (to be run by SWH Celery workers) and the
loader class. For example, for the Subversion loader::

   from typing import Any, Dict

   def register() -> Dict[str, Any]:
       from swh.loader.svn.loader import SvnLoader

       return {
           "task_modules": ["%s.tasks" % __name__],
           "loader": SvnLoader,
       }

The bulk of the work is done by the returned ``loader`` class: it loads
artifacts from the upstream VCS and writes them to the |swh| archive.
Because of the heterogeneity of VCS loaders, it has a lot of freedom in how to
achieve this. Once the initial setup is done (see the next section), its ``load``
method is called, and it is expected to do all this work as a black box.

.. _swh-py-template: https://forge.softwareheritage.org/source/swh-py-template/
.. _entrypoint: https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html

Base classes
------------

All loaders inherit from :class:`swh.loader.core.loader.BaseLoader`, which takes care of
all the SWH-specific setup and finalization:

* Reading the configuration
* Connecting to the :term:`storage database`
* Storing :term:`origin` and :term:`visit` objects

It also provides a default implementation of the ``load`` method, which takes care of:

* calling its ``fetch_data`` (from the VCS) and ``store_data`` (to SWH) in a loop
* on error, notifies swh-storage the loading failed, reports the error to
  the monitoring infrastructure (Sentry), and cleanup
* on success, cleanup and notify swh-storage the loading succeeded

See :meth:`its documentation <swh.loader.core.loader.BaseLoader.load>` for details.

Distributed VCS loaders will usually want to inherit from its child,
:class:`swh.loader.core.DVCSLoader`, which takes care of implementing ``store_data``.
Classes inheriting from ``DVCSLoader`` only need to implement ``fetch_data``, and
a method for each object type: ``get_contents``, ``get_directories``, ``get_revisions``,
``get_releases``, and ``get_snapshot``, each returning an iterable of the corresponding
object from :mod:`swh.model.model`
(except ``get_snapshot``, which returns a single one).

If you are writing a DVCS loader, this allows your loader to fetch all the objects
locally, then return them lazily on demand.


Incremental loading
-------------------

Loading a repository from scratch can be costly, so ``swh-storage`` provides
ways to remember what objects in the repository were already loaded,
through :term:`extids <extid>`.
They are represented by :class:`swh.model.model.ExtID`,
which is essentially a 3-tuple that contains a SWHID, an id internal to the VCS type,
(which is the actual "extid" itself), and the type of this id (eg. ``hg-nodeid``).

When your loader is done loading, it can store extids for some of its objects
(eg. the heads/tips of each branch of the :term:`snapshot` and some intermediate
revisions in the history),
with :meth:`swh.storage.interface.StorageInterface.extid_add`.

And when it starts loading a known repository, fetches the previous snapshot
using :func:`swh.storage.algos.snapshot.snapshot_get_latest`, then the extids
it stores using :meth:`swh.storage.interface.StorageInterface.extid_get_from_target`
for each of the branch targets.
This way, it can find which objects from the origin were already loaded,
without having to download them first.

.. note::

   For legacy reasons, the Subversion loader uses an alternative to ExtID,
   which is to encode the repository UUID and the revision ID (an incremental integer)
   directly in :attr:`swh.model.model.Revision.extra_headers`.

   This is discouraged because it prevents deduplication across repositories,
   and ``extra_headers`` does not have a well-defined schema.

Integrity
---------

Loaders may be interrupted at any point, for various reasons (unhandled crash,
out of memory, hardware failure, blocking IO, system or daemon restart, etc.)

Therefore, they must take great care that if a load was interrupted, the next load
will finish loading all objects. If they don't, this may happen:

1. loader loads revision ``R``, pointing to directory ``D``
2. loader starts loading ``D``, but crashes before it does
3. [loader restarts]
4. loader sees ``R`` is already loaded, so it doesn't load its children

And ``D`` will never be loaded.

The solution to this is to load objects in topological order of the DAG.

Another reason to load objects in topological order is that it avoid having "holes"
in the graph (aka. dangling references), even temporarily.
Holes in the graph cause bad user experiences, when users click a link from
an existing object and get a "not found" error.
