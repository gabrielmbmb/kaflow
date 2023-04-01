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

.PHONY: all
all: format lint mypy test
