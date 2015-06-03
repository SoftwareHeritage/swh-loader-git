REPO_PATH=$(HOME)/work/inria/repo/org-beamer-swh
FLAG=

prepare:
	mkdir -p log dataset

run: prepare
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) --repo-path $(REPO_PATH) initdb

clean:
	rm -rf ./dataset/
	PYTHONPATH=`pwd` ./bin/sgloader $(FLAG) cleandb
