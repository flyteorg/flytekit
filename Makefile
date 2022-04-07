export REPOSITORY=flytekit

PIP_COMPILE = pip-compile --upgrade --verbose
MOCK_FLYTE_REPO=tests/flytekit/integration/remote/mock_flyte_repo/workflows

.SILENT: help
.PHONY: help
help:
	echo Available recipes:
	cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' | awk 'BEGIN { FS = ":.*?## " } { cnt++; a[cnt] = $$1; b[cnt] = $$2; if (length($$1) > max) max = length($$1) } END { for (i = 1; i <= cnt; i++) printf "  $(shell tput setaf 6)%-*s$(shell tput setaf 0) %s\n", max, a[i], b[i] }'
	tput sgr0

.PHONY: install-piptools
install-piptools:
	pip install -U pip-tools setuptools wheel "pip>=22.0.3"

.PHONY: update_boilerplate
update_boilerplate:
	@curl https://raw.githubusercontent.com/flyteorg/boilerplate/master/boilerplate/update.sh -o boilerplate/update.sh
	@boilerplate/update.sh

.PHONY: setup
setup: install-piptools ## Install requirements
	pip-sync requirements.txt dev-requirements.txt

.PHONY: setup-spark2
setup-spark2: install-piptools ## Install requirements
	pip-sync requirements-spark2.txt dev-requirements.txt

.PHONY: fmt
fmt: ## Format code with black and isort
	pre-commit run black --all-files || true
	pre-commit run isort --all-files || true

.PHONY: lint
lint: ## Run linters
	mypy flytekit/core || true
	mypy flytekit/types || true
	mypy tests/flytekit/unit/core || true
	# Exclude setup.py to fix error: Duplicate module named "setup"
	mypy plugins --exclude setup.py || true
	pre-commit run --all-files

.PHONY: spellcheck
spellcheck:  ## Runs a spellchecker over all code and documentation
	codespell -L "te,raison,fo" --skip="./docs/build,./.git"

.PHONY: test
test: lint unit_test

.PHONY: unit_test
unit_test:
	pytest -m "not sandbox_test" tests/flytekit/unit

requirements-spark2.txt: export CUSTOM_COMPILE_COMMAND := make requirements-spark2.txt
requirements-spark2.txt: requirements-spark2.in install-piptools
	$(PIP_COMPILE) $<

requirements.txt: export CUSTOM_COMPILE_COMMAND := make requirements.txt
requirements.txt: requirements.in install-piptools
	$(PIP_COMPILE) $<

dev-requirements.txt: export CUSTOM_COMPILE_COMMAND := make dev-requirements.txt
dev-requirements.txt: dev-requirements.in requirements.txt install-piptools
	$(PIP_COMPILE) $<

doc-requirements.txt: export CUSTOM_COMPILE_COMMAND := make doc-requirements.txt
doc-requirements.txt: doc-requirements.in install-piptools
	$(PIP_COMPILE) $<

${MOCK_FLYTE_REPO}/requirements.txt: export CUSTOM_COMPILE_COMMAND := make ${MOCK_FLYTE_REPO}/requirements.txt
${MOCK_FLYTE_REPO}/requirements.txt: ${MOCK_FLYTE_REPO}/requirements.in install-piptools
	$(PIP_COMPILE) $<

.PHONY: requirements
requirements: requirements.txt dev-requirements.txt requirements-spark2.txt doc-requirements.txt ${MOCK_FLYTE_REPO}/requirements.txt ## Compile requirements

# TODO: Change this in the future to be all of flytekit
.PHONY: coverage
coverage:
	coverage run -m pytest tests/flytekit/unit/core flytekit/types -m "not sandbox_test"
	coverage report -m --include="flytekit/core/*,flytekit/types/*"

PLACEHOLDER := "__version__\ =\ \"0.0.0+develop\""

.PHONY: update_version
update_version:
	# ensure the placeholder is there. If grep doesn't find the placeholder
	# it exits with exit code 1 and github actions aborts the build.
	grep "$(PLACEHOLDER)" "flytekit/__init__.py"
	sed -i "s/$(PLACEHOLDER)/__version__ = \"${VERSION}\"/g" "flytekit/__init__.py"

	grep "$(PLACEHOLDER)" "setup.py"
	sed -i "s/$(PLACEHOLDER)/__version__ = \"${VERSION}\"/g" "setup.py"
