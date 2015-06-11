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
    NB_COMMITS=$(data "commit")
    NB_TREES=$(data "tree")
    NB_BLOBS=$(data "blob")
    NB_TAGS=$(data "tag")
    cat <<EOC
commit $NB_COMMITS
tree $NB_TREES
blob $NB_BLOBS
tag $NB_TAGS
EOC
else
    NUM=$(data $TYPE)
    echo "$TYPE $NUM"
fi
