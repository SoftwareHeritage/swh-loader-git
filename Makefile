REPO_PATH=$(HOME)/repo/perso/dot-files

prepare:
	mkdir -p log dataset

run: prepare
	PYTHONPATH=`pwd` PYTHONLOG=DEBUG ./bin/sgloader --repo-path $(REPO_PATH) createdb

clean:
	rm -rf ./dataset/
	PYTHONPATH=`pwd` ./bin/sgloader dropdb
