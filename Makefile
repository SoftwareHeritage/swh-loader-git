REPO_PATH=$(HOME)/repo/perso/dot-files

prepare:
	mkdir -p log dataset

run: prepare
	PYTHONPATH=`pwd` ./bin/sgloader --repo-path $(REPO_PATH) createdb

clean:
	PYTHONPATH=`pwd` ./bin/sgloader dropdb
