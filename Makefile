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

clean:
	rm -rf /tmp/swh-git-loader/content-storage

cleandb: clean
	PYTHONPATH=`pwd` $(SWH_DB_MANAGER) $(FLAG) cleandb

initdb: clean
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

test-run-back:
	PYTHONPATH=`pwd` $(SWH_BACK) $(FLAG) --config ./resources/test/back.ini

test:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)

test-client:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_client.py

test-swhmap:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_swhmap.py

test-loader:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_loader.py

test-api:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api*.py

test-api-post-per-type:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_post_*.py

test-api-object:
	$(NOSE) $(TESTFLAGS) $(TESTDIR)/test_api_object.py

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

readme:
	pandoc -f org -t markdown README.org > README

log-loader:
	tail $(FOLLOW_LOG) /tmp/swh-git-loader/log/sgloader.log

log-back:
	tail $(FOLLOW_LOG) /tmp/swh-git-loader/log/back.log

coverage:
	$(NOSE) --with-coverage $(SRCDIR) -v --cover-package=$(SRCDIR)
