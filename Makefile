#!/bin/bash
VENVBIN=./venv/bin
NODEBIN=./node_modules/.bin
PYTHON=$(VENVBIN)/python
PIP=$(VENVBIN)/pip
PYTEST=$(VENVBIN)/py.test
PYLINT=$(VENVBIN)/pylint
DIR=/usr/share/geoip2

all: env lint test

env:
	virtualenv -p python3 venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	make geoip

dev:

	source venv/bin/activate

geoip: 
	sudo mkdir -p $(DIR)
	sudo wget https://s3-ap-southeast-1.amazonaws.com/files.dev.urbanindo.com/2018/03/GeoIP2-City_20180227/GeoIP2-City.mmdb -O $(DIR)/GeoIP2-City.mmdb



clean:
	find . -name "*.pyc" -exec rm -rf {} \;
	rm -rf venv
	rm -rf vendored

lint:
	$(PYLINT) meta_tag/
	$(PYLINT) handler.py

test: 
	pytest meta_tag/test_generate_meta.py 
