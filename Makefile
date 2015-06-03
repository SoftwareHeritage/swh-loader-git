REPO_PATH=$(HOME)/repo/perso/dot-files
FLAG=

prepare:
	mkdir -p log dataset

run: prepare
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) --repo-path $(REPO_PATH) createdb

clean:
	rm -rf ./dataset/
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) dropdb
