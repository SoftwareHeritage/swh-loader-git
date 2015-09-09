#!/usr/bin/env python3

import pygit2
from pygit2 import GIT_SORT_TOPOLOGICAL

import sys

repo_path = sys.argv[1]
ref_name = sys.argv[2]

repo = pygit2.Repository(repo_path)

ref = repo.lookup_reference(ref_name)

head_rev = repo[ref.target]

for rev in repo.walk(head_rev.hex, GIT_SORT_TOPOLOGICAL):
    print(rev.hex, rev.tree.hex)
    for tree_entry in rev.tree:
        print(repo.get(tree_entry.oid))
