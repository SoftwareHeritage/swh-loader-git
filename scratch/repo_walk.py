#!/usr/bin/env python3


import pygit2
from pygit2 import GIT_OBJ_COMMIT, GIT_OBJ_TREE, GIT_SORT_TOPOLOGICAL

import sys


repo_path = sys.argv[1]
ref_name = sys.argv[2]

repo = pygit2.Repository(repo_path)

ref = repo.lookup_reference(ref_name)

head_rev = repo[ref.target]

for rev in repo.walk(head_rev.hex, GIT_SORT_TOPOLOGICAL):
    print("revision: %s, revision.tree: %s" % (rev.hex, rev.tree.hex))
    for tree_entry in rev.tree:
        obj = repo.get(tree_entry.oid)
        print("tree_entry: %s, obj: %s" % (tree_entry, obj))
        if obj is None:  #.type == GIT_OBJ_COMMIT: -> submodule are not in repo object
            print('submodule revision: %s' % tree_entry.hex)
        elif obj.type == GIT_OBJ_TREE:
            print('tree')
        else:
            print('blob')
