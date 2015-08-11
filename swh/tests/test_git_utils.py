# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pygit2


def create_blob(git_repo, blob_content):
    """Create a blob with blob_content and returns its oid.
    """
    return git_repo.create_blob(blob_content)


def create_tree(git_repo, blob_content=None):
    """Create a tree.
    If blob_content is specified, create a blob then
    create a tree which points to this blob.
    Returns the tree's oid.
    """
    treeBuilder = git_repo.TreeBuilder()
    if blob_content:
        new_blob = create_blob(git_repo, blob_content)
        treeBuilder.insert('blob', new_blob,
                           pygit2.GIT_FILEMODE_BLOB_EXECUTABLE)
    return treeBuilder.write()


def create_author_and_committer():
    """Create a dummy signature for author and committer.
    """
    author = pygit2.Signature('Alice Cooper',
                              'alice@cooper.tld')
    committer = pygit2.Signature('Vincent Furnier',
                                 'vincent@committers.tld')
    return (author, committer)


def create_commit_with_content(git_repo,
                               blob_content,
                               commit_msg,
                               commit_parents=None):
    """Create a commit inside the git repository and return its oid.
    """
    author, committer = create_author_and_committer()
    tree = create_tree(git_repo, blob_content)
    return git_repo.create_commit(
        'refs/heads/master',  # the name of the reference to update
        author, committer, commit_msg,
        tree,  # binary string representing the tree object ID
        [] if commit_parents is None else commit_parents  # commit parents
    )
