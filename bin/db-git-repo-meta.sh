#!/usr/bin/env bash

# Use: $0 <DB-name>
# will compute the number of revision, directory, content from db respectively.

DB=$1

count() {
    DB=$1
    QUERY=$2
    psql -d $1 --command "$QUERY;" | tail -3 | head -1
}

NB_CONTENTS=$(count $DB "select count(*) from content;")
NB_DIRECTORIES=$(count $DB "select count(*) from directory;")
NB_REVISIONS=$(count $DB "select count(*) from revision;")
NB_RELEASES=$(count $DB "select count(*) from release;")

cat<<EOF
content   $NB_CONTENTS
directory $NB_DIRECTORIES
revision  $NB_REVISIONS
release   $NB_RELEASES
EOF
