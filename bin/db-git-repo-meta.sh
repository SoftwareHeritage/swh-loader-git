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
NB_DIRECTORY_ENTRIES=$(count $DB "select count(*) from directory_entry;")
NB_REVISIONS=$(count $DB "select count(*) from revision;")
NB_RELEASES=$(count $DB "select count(*) from release;")
NB_PERSONS=$(count $DB "select count(*) from person;")

cat<<EOF
content           $NB_CONTENTS
directory         $NB_DIRECTORIES
directory_entries $NB_DIRECTORY_ENTRIES
revision          $NB_REVISIONS
release           $NB_RELEASES
person            $NB_PERSONS
EOF
