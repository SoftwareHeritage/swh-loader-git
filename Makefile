FLAKE = flake8
BINDIR = bin
SRCDIR = sgloader
REPO_PATH=$(HOME)/work/inria/repo/org-beamer-swh

# add -v for example
FLAG=

NOSE = nosetests
TESTFLAGS = -s
TESTDIR = ./tests

prepare:
	mkdir -p log dataset

clean:
	rm -rf ./log
	rm -rf ./dataset/

cleandb: clean prepare
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) cleandb

run: clean prepare
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) --repo-path $(REPO_PATH) initdb

check:
	$(FLAKE) $(BINDIR)/sgloader $(SRCDIR)/*.py

profile:
	python3 -m cProfile profile.py

test:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)
