FLAKE = flake8
BINDIR = bin
SRCDIR = sgloader
REPO_PATH=$(HOME)/work/inria/repo/org-beamer-swh

# add -v for example
FLAG=

NOSE = nosetests3
TESTFLAGS = -s
TESTDIR = ./tests

DB=swhgitloader
DB_TEST=swhgitloader-test

deps:
	sudo apt-get install -y python3 python3-pygit2 python3-psycopg2 python3-nose

prepare:
	mkdir -p log dataset

clean:
	rm -rf ./log
	rm -rf ./dataset/

cleandb: clean prepare
	PYTHONPATH=`pwd` $(BINDIR)/sgloader $(FLAG) cleandb

run: clean prepare
	PYTHONPATH=`pwd` $(BINDIR)/sgloader $(FLAG) --repo-path $(REPO_PATH) initdb

check:
	$(FLAKE) $(BINDIR)/sgloader $(SRCDIR)/*.py

profile:
	python3 -m cProfile profile.py

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
