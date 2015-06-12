FLAKE = flake8
BINDIR = bin
SRCDIR = swh
REPO_PATH=../debsources

# add -v for example
FLAG=

NOSE = nosetests3
TESTFLAGS = -s
TESTDIR = ./swh/tests

DB=swhgitloader
DB_TEST=swhgitloader-test

BIN=$(BINDIR)/swh-git-loader

deps:
	sudo apt-get install -y python3 python3-pygit2 python3-psycopg2 python3-nose ipython3

prepare:
	mkdir -p swh-git-loader/log swh-git-loader/file-content-storage swh-git-loader/object-content-storage

clean:
	rm -rf swh-git-loader/log swh-git-loader/file-content-storage swh-git-loader/object-content-storage

help: clean prepare
	PYTHONPATH=`pwd` $(BIN) $(FLAG) -h

cleandb: clean prepare
	PYTHONPATH=`pwd` $(BIN) $(FLAG) cleandb

initdb: clean prepare
	PYTHONPATH=`pwd` $(BIN) $(FLAG) initdb

run:
	PYTHONPATH=`pwd` $(BIN) $(FLAG) load $(REPO_PATH)

clean-and-run: clean prepare
	PYTHONPATH=`pwd` $(BIN) $(FLAG) cleandb
	PYTHONPATH=`pwd` $(BIN) $(FLAG) initdb
	PYTHONPATH=`pwd` $(BIN) $(FLAG) load $(REPO_PATH)

check:
	$(FLAKE) $(BINDIR) $(SRCDIR)

profile:
	[ -f profile-sgloader.py ] && python3 -m cProfile profile-sgloader.py

test:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)

test-connect-db:
	psql -d $(DB_TEST)

test-drop-db:
	sudo su -l postgres -c "dropdb $(DB_TEST)"

test-create-db:
	sudo su -l postgres -c "createdb -O $(USER) $(DB_TEST)"

connect-db:
	psql -d $(DB)

drop-db:
	sudo su -l postgres -c "dropdb $(DB)"

create-db:
	sudo su -l postgres -c "createdb -O $(USER) $(DB)"

check-meta:
	echo "Repository metadata:"
	$(BINDIR)/dir-git-repo-meta.sh $(REPO_PATH)

	echo "DB Repository metadata:"
	$(BINDIR)/db-git-repo-meta.sh

readme:
	pandoc -f org -t markdown README.org > README
