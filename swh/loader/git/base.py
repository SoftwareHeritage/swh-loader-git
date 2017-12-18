# Copyright (C) 2016-2017 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import os

from swh.loader.core.loader import SWHLoader


class BaseLoader(SWHLoader):
    """This base class is a pattern for loaders.

    The external calling convention is as such:

    - instantiate the class once (loads storage and the configuration)
    - for each origin, call load with the origin-specific arguments (for
      instance, an origin URL).

    load calls several methods that must be implemented in subclasses:

    - prepare(\*args, \**kwargs) prepares the loader for the new origin
    - get_origin gets the origin object associated to the current loader
    - fetch_data downloads the necessary data from the origin
    - get_{contents,directories,revisions,releases,occurrences} retrieve each
      kind of object from the origin
    - has\_* checks whether there are some objects to load for that object type
    - get_fetch_history_result retrieves the data to insert in the
      fetch_history table once the load was successful
    - cleanup cleans up an eventual state installed for computations
    - eventful returns whether the load was eventful or not

    """
    DEFAULT_CONFIG = {
        'storage': ('dict', {
            'cls': 'remote',
            'args': {
              'url': 'http://localhost:5002/'
            },
        }),
        'send_contents': ('bool', True),
        'send_directories': ('bool', True),
        'send_revisions': ('bool', True),
        'send_releases': ('bool', True),
        'send_occurrences': ('bool', True),

        'save_data': ('bool', False),
        'save_data_path': ('str', ''),

        'content_packet_size': ('int', 10000),
        'content_packet_size_bytes': ('int', 1024 * 1024 * 1024),
        'directory_packet_size': ('int', 25000),
        'revision_packet_size': ('int', 100000),
        'release_packet_size': ('int', 100000),
        'occurrence_packet_size': ('int', 100000),
    }

    def __init__(self):
        super().__init__(logging_class='swh.loader.git.BulkLoader')

        # Make sure the config is sane
        if self.config['save_data']:
            path = self.config['save_data_path']
            os.stat(path)
            if not os.access(path, os.R_OK | os.W_OK):
                raise PermissionError("Permission denied: %r" % path)

        self.visit_date = None  # possibly overridden in self.prepare method

    @abc.abstractmethod
    def has_contents(self):
        """Checks whether we need to load contents"""
        pass

    def get_contents(self):
        """Get the contents that need to be loaded"""
        raise NotImplementedError

    @abc.abstractmethod
    def has_directories(self):
        """Checks whether we need to load directories"""
        pass

    def get_directories(self):
        """Get the directories that need to be loaded"""
        raise NotImplementedError

    @abc.abstractmethod
    def has_revisions(self):
        """Checks whether we need to load revisions"""

    def get_revisions(self):
        """Get the revisions that need to be loaded"""
        raise NotImplementedError

    @abc.abstractmethod
    def has_releases(self):
        """Checks whether we need to load releases"""
        return True

    def get_releases(self):
        """Get the releases that need to be loaded"""
        raise NotImplementedError

    @abc.abstractmethod
    def has_occurrences(self):
        """Checks whether we need to load occurrences"""
        pass

    def get_occurrences(self):
        """Get the occurrences that need to be loaded"""
        raise NotImplementedError

    def get_fetch_history_result(self):
        """Return the data to store in fetch_history for the current loader"""
        raise NotImplementedError

    def eventful(self):
        """Whether the load was eventful"""
        raise NotImplementedError

    def save_data(self):
        """Save the data associated to the current load"""
        raise NotImplementedError

    def get_save_data_path(self):
        """The path to which we save the data"""
        if not hasattr(self, '__save_data_path'):
            origin_id = self.origin_id
            year = str(self.visit_date.year)

            path = os.path.join(
                self.config['save_data_path'],
                "%04d" % (origin_id % 10000),
                "%08d" % origin_id,
                year,
            )

            os.makedirs(path, exist_ok=True)
            self.__save_data_path = path

        return self.__save_data_path

    def cleanup(self):
        """Clean up an eventual state installed for computations.
           Nothing specific for the loader-git is needed.

        """
        pass

    def store_data(self):
        """Store data fetched from the git repository.

        """
        if self.config['save_data']:
            self.save_data()

        if self.config['send_contents'] and self.has_contents():
            self.send_batch_contents(self.get_contents())
        if self.config['send_directories'] and self.has_directories():
            self.send_batch_directories(self.get_directories())
        if self.config['send_revisions'] and self.has_revisions():
            self.send_batch_revisions(self.get_revisions())
        if self.config['send_releases'] and self.has_releases():
            self.send_batch_releases(self.get_releases())
        if self.config['send_occurrences'] and self.has_occurrences():
            self.send_batch_occurrences(self.get_occurrences())
