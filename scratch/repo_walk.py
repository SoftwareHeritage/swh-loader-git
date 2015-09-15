#!/usr/bin/env python3


import pygit2
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_SORT_TOPOLOGICAL

import sys


repo_path = sys.argv[1]

repo = pygit2.Repository(repo_path)

for ref_name in repo.listall_references():
    print(ref_name)
    ref = repo.lookup_reference(ref_name)
    head_rev = repo[ref.target]  # noqa

    for rev in repo.walk(head_rev.hex, GIT_SORT_TOPOLOGICAL):
        for tree_entry in rev.tree:
            obj = repo.get(tree_entry.oid)
            if obj is None:  #.type == GIT_OBJ_COMMIT: -> submodule are not in repo object
                print('subr: %s' % tree_entry.hex)
            elif obj.type == GIT_OBJ_TREE:
                print('tree: %s' % tree_entry.hex)
            else:
                print('blob: %s' % tree_entry.hex)
