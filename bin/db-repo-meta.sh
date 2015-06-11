#!/usr/bin/env bash

count() {
    QUERY=$1
    psql -d swhgitloader --command "$QUERY;" | tail -3 | head -1
}

NB_COMMITS=$(count "select count(*) from git_objects where type='commit';")
NB_TREES=$(count "select count(*) from git_objects where type='tree';")
NB_BLOB=$(count "select count(*) from files;")

cat<<EOF
commit $NB_COMMITS
tree $NB_TREES
blob $NB_BLOB
EOF
