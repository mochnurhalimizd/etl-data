
VENVBIN=./venv/bin
NODEBIN=./node_modules/.bin
PYTHON=$(VENVBIN)/python
PIP=$(VENVBIN)/pip
PYTEST=$(VENVBIN)/py.test
PYLINT=$(VENVBIN)/pylint

all: env lint test

env:
	virtualenv -p python3 venv
	npm install
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

dev:
	. ./venv/bin/activate

clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	rm -rf venv
	rm -rf vendored
	rm -rf node_modules

lint:
	$(PYLINT) meta_tag/
	$(PYLINT) handler.py

test: 
	pytest meta_tag/test_generate_meta.py 
