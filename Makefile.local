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

# Adapt python-path to use other modules
_PYPATH=`pwd`:`pwd`/../swh-core

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

clean:
	rm -rf /tmp/swh-git-loader/content-storage

cleandb: clean
	PYTHONPATH=$(_PYPATH) $(SWH_DB_MANAGER) $(FLAG) cleandb

run-remote:
	PYTHONPATH=`pwd`:`pwd`/../swh-core $(SWH_LOADER) $(FLAG) --config ./resources/remote-git-loader.ini load $(REPO_PATH)

run-local:
	PYTHONPATH=$(_PYPATH) $(SWH_LOADER) $(FLAG) --config ./resources/local-git-loader.ini load $(REPO_PATH)

run:
	# works with the default ~/.config/swh/git-loader.ini file
	PYTHONPATH=$(_PYPATH) $(SWH_LOADER) $(FLAG) load $(REPO_PATH)

run-back:
	PYTHONPATH=$(_PYPATH) $(SWH_BACK) $(FLAG)

check:
	$(FLAKE) $(BINDIR) $(SRCDIR)

profile-run:
	PYTHONPATH=$(_PYPATH) python3 -m $(PROFILE_TYPE) -o ./scratch/swhgitloader.$(PROFILE_TYPE) ./scratch/profile-swhgitloader.py

profile-stats:
	PYTHONPATH=$(_PYPATH) ./scratch/analyse-profile.py

test-run-back:
	PYTHONPATH=$(_PYPATH) $(SWH_BACK) $(FLAG) --config ./resources/test/back.ini

test:
	PYTHONPATH=$(_PYPATH) $(NOSE) $(TESTFLAGS) $(TESTDIR)

test-remote-loader:
	PYTHONPATH=$(_PYPATH) $(NOSE) $(TESTFLAGS) $(TESTDIR)/test_remote_loader.py

test-local-loader:
	PYTHONPATH=$(_PYPATH) $(NOSE) $(TESTFLAGS) $(TESTDIR)/test_local_loader.py

test-http:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_http.py

test-swhrepo:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_swhrepo.py

test-api:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api*.py

test-api-post-per-type:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_post_*.py

test-api-content:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_content.py

test-api-directory:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_directory.py

test-api-revision:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_revision.py

test-api-release:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_release.py

test-api-occurrence:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_occurrence.py

test-api-home:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_home.py

test-api-origin:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_origin.py

test-api-person:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_person.py

test-api-pickle:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_pickle.py

test-file:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_file.py

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
	@$(BINDIR)/db-git-repo-meta.sh $(DB) $(REPO_PATH)
	@echo

log-loader:
	tail $(FOLLOW_LOG) /tmp/swh-git-loader/log/sgloader.log

log-back:
	tail $(FOLLOW_LOG) /tmp/swh-git-loader/log/back.log

coverage:
	PYTHONPATH=$(_PYPATH) $(NOSE) --with-coverage $(SRCDIR) -v --cover-package=$(SRCDIR)
