define PIP_COMPILE
pip-compile $(1) --upgrade --verbose
endef

.SILENT: help
.PHONY: help
help:
	echo Available recipes:
	cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' | awk 'BEGIN { FS = ":.*?## " } { cnt++; a[cnt] = $$1; b[cnt] = $$2; if (length($$1) > max) max = length($$1) } END { for (i = 1; i <= cnt; i++) printf "  $(shell tput setaf 6)%-*s$(shell tput setaf 0) %s\n", max, a[i], b[i] }'
	tput sgr0

.PHONY: install-piptools
install-piptools:
	pip install -U pip-tools

.PHONY: setup
setup: install-piptools ## Install requirements
	pip-sync requirements.txt dev-requirements.txt

.PHONY: setup-spark3
setup-spark3: install-piptools ## Install requirements
	pip-sync requirements-spark3.txt dev-requirements.txt

.PHONY: fmt
fmt: ## Format code with black and isort
	black .
	isort .

.PHONY: lint
lint: ## Run linters
	mypy || true
	flake8 .

.PHONY: test
test: lint ## Run tests
	pytest tests/flytekit/unit
	pytest tests/scripts
	pytest plugins/tests
	shellcheck **/*.sh

requirements-spark3.txt: export CUSTOM_COMPILE_COMMAND := make requirements-spark3.txt
requirements-spark3.txt: requirements-spark3.in install-piptools
	$(call PIP_COMPILE,requirements-spark3.in)

requirements.txt: export CUSTOM_COMPILE_COMMAND := make requirements.txt
requirements.txt: requirements.in install-piptools
	$(call PIP_COMPILE,requirements.in)

dev-requirements.txt: export CUSTOM_COMPILE_COMMAND := make dev-requirements.txt
dev-requirements.txt: dev-requirements.in requirements.txt install-piptools
	$(call PIP_COMPILE,dev-requirements.in)

.PHONY: requirements
requirements: requirements.txt dev-requirements.txt requirements-spark3.txt ## Compile requirements

# TODO: Change this in the future to be all of flytekit
.PHONY: coverage
coverage:
	coverage run -m pytest tests/flytekit/unit/annotated flytekit/types plugins/tests
	coverage report -m --include="flytekit/annotated/*,flytekit/types/*,plugins/*"
