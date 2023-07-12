.DEFAULT_GOAL := all
sources = kaflow tests

.PHONY: .pdm
.pdm:
	@pdm -V || echo 'Please install PDM: https://pdm.fming.dev/latest/\#installation'

.PHONY: install
install: .pdm
	pdm install
	pre-commit install

.PHONY: format
format:
	pdm run black $(sources)
	pdm run ruff --fix $(sources)

.PHONY: lint
lint:
	pdm run black --check $(sources)
	pdm run ruff $(sources)

.PHONY: mypy
mypy:
	pdm run mypy kaflow

.PHONY: test
test:
	pdm run coverage run -m pytest

.PHONY: cov-html
cov-html: test
	pdm run coverage html

.PHONY: start-kafka
start-kafka:
	docker-compose up -d

.PHONY: stop-kafka
stop-kafka:
	docker-compose down -v

.PHONY: all
all: format lint mypy test
