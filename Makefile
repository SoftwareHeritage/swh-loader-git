FLAKE = flake8
BINDIR = bin
SRCDIR = swh
REPO_PATH=../debsources

# add -v for example
FLAG=

NOSE = nosetests3
TESTFLAGS = -s
TESTDIR = ./swh/tests

DB=softwareheritage-dev
DB_TEST=$(DB)-test

SWH_LOADER=$(BINDIR)/swh-git-loader
SWH_DB_MANAGER=$(BINDIR)/swh-db-manager
SWH_BACK=$(BINDIR)/swh-backend

# could use cProfile
PROFILE_TYPE=profile

FOLLOW_LOG=-f

deps:
	apt-get install -y \
		python3 \
		python3-pygit2 \
		python3-psycopg2 \
		python3-nose \
		python3-flask \
		python3-requests \
		python3-retrying \
		ipython3

prepare:
	mkdir -p /tmp/swh-git-loader/log /tmp/swh-git-loader/content-storage

clean:
	rm -rf /tmp/swh-git-loader/log /tmp/swh-git-loader/content-storage

cleandb: clean prepare
	PYTHONPATH=`pwd` $(SWH_DB_MANAGER) $(FLAG) cleandb

initdb: clean prepare
	PYTHONPATH=`pwd` $(SWH_DB_MANAGER) $(FLAG) initdb

run:
	PYTHONPATH=`pwd` $(SWH_LOADER) $(FLAG) load $(REPO_PATH)

run-back:
	PYTHONPATH=`pwd` $(SWH_BACK) $(FLAG)

clean-and-run: cleandb initdb
	PYTHONPATH=`pwd` $(SWH_LOADER) $(FLAG) load $(REPO_PATH)

check:
	$(FLAKE) $(BINDIR) $(SRCDIR)

profile-run:
	PYTHONPATH=`pwd` python3 -m $(PROFILE_TYPE) -o ./scratch/swhgitloader.$(PROFILE_TYPE) ./scratch/profile-swhgitloader.py

profile-stats:
	PYTHONPATH=`pwd` ./scratch/analyse-profile.py

test:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)

test-loader:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_loader.py

test-api:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_backend.py

connect-db:
	psql -d $(DB)

create-db:
	cd ../swh-sql && make clean initdb

drop-db:
	cd ../swh-sql && make clean dropdb

test-connect-db:
	psql -d $(DB_TEST)

test-create-db:
	cd ../swh-sql && make clean initdb DBNAME=$(DB_TEST)

test-drop-db:
	cd ../swh-sql && make clean dropdb DBNAME=$(DB_TEST)

check-meta:
	@echo "Repository: $(REPO_PATH)"

	@echo "Git metadata:"
	@$(BINDIR)/dir-git-repo-meta.sh $(REPO_PATH)
	@echo

	@echo "DB metadata:"
	@$(BINDIR)/db-git-repo-meta.sh $(REPO_PATH)
	@echo

readme:
	pandoc -f org -t markdown README.org > README

log-loader:
	tail $(FOLLOW_LOG) swh-git-loader/log/sgloader.log

log-back:
	tail $(FOLLOW_LOG) swh-git-loader/log/back.log

coverage:
	$(NOSE) --with-coverage $(SRCDIR) -v --cover-package=$(SRCDIR)
