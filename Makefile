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

# could use cProfile
PROFILE_TYPE=profile

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

profile-run:
	PYTHONPATH=`pwd` python3 -m $(PROFILE_TYPE) -o ./scratch/swhgitloader.$(PROFILE_TYPE) ./scratch/profile-swhgitloader.py

profile-stats:
	PYTHONPATH=`pwd` ./scratch/analyse-profile.py

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
	@echo "Git Repository metadata:"
	@$(BINDIR)/dir-git-repo-meta.sh $(REPO_PATH)
	@echo

	@echo "DB Repository metadata:"
	@$(BINDIR)/db-git-repo-meta.sh
	@echo

readme:
	pandoc -f org -t markdown README.org > README

log:
	tail -f swh-git-loader/log/sgloader.log

coverage:
	$(NOSE) --with-coverage $(SRCDIR) -v --cover-package=$(SRCDIR)

run-back:
	PYTHONPATH=`pwd` $(SRCDIR)/backend/back.py
