.DEFAULT_GOAL := all
sources = kaflow tests

.PHONY: install
install: 
	pip install -e .[dev,test]

.PHONY: format
format:
	black --preview $(sources)
	ruff --fix $(sources)

.PHONY: lint
lint:
	black --preview --check $(sources)
	ruff $(sources)

.PHONY: mypy
mypy:
	mypy kaflow

.PHONY: test
test:
	coverage run -m pytest

.PHONY: cov_html
cov_html: test
	coverage html

.PHONY: start-kafka
start-kafka:
	docker-compose up -d

.PHONY: stop-kafka
stop-kafka:
	docker-compose down -v

.PHONY: all
all: format lint mypy test
