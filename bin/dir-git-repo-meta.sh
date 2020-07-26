#!/usr/bin/env bash

# count the number of type (tree, blob, tag, commit)
REPO=${1-`pwd`}
TYPE=${2-"all"}

data() {
    git rev-list --objects --all \
             | git cat-file --batch-check='%(objectname) %(objecttype) %(rest)' \
             | cut -f2 -d' ' \
             | grep $1 \
             | wc -l
}

cd $REPO

if [ "$TYPE" = "all" ]; then
    NB_BLOBS=$(data "blob")
    NB_TREES=$(data "tree")
    NB_COMMITS=$(data "commit")
    NB_TAGS=$(data "tag")
    cat <<EOC
blob   $NB_BLOBS
tree   $NB_TREES
commit $NB_COMMITS
tag    $NB_TAGS
EOC
else
    NUM=$(data $TYPE)
    echo "$TYPE $NUM"
fi
