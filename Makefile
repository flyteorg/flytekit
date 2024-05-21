export REPOSITORY=flytekit

PIP_COMPILE = pip-compile --upgrade --verbose --resolver=backtracking
MOCK_FLYTE_REPO=tests/flytekit/integration/remote/mock_flyte_repo/workflows
PYTEST_OPTS ?= -n auto --dist=loadfile
PYTEST_AND_OPTS = pytest ${PYTEST_OPTS}
PYTEST = pytest

.SILENT: help
.PHONY: help
help:
	echo Available recipes:
	cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' | awk 'BEGIN { FS = ":.*?## " } { cnt++; a[cnt] = $$1; b[cnt] = $$2; if (length($$1) > max) max = length($$1) } END { for (i = 1; i <= cnt; i++) printf "  $(shell tput setaf 6)%-*s$(shell tput setaf 0) %s\n", max, a[i], b[i] }'
	tput sgr0

.PHONY: install-piptools
install-piptools:
	# pip 22.1 broke pip-tools: https://github.com/jazzband/pip-tools/issues/1617
	python -m pip install -U pip-tools setuptools wheel "pip>=22.0.3,!=22.1"

.PHONY: update_boilerplate
update_boilerplate:
	@curl https://raw.githubusercontent.com/flyteorg/boilerplate/master/boilerplate/update.sh -o boilerplate/update.sh
	@boilerplate/update.sh

.PHONY: setup
setup: install-piptools ## Install requirements
	pip install -r dev-requirements.in

# Warning: this will install the requirements in your system python
.PHONY: setup-global-uv
setup-global-uv:
#   Use "dev0" prefix to emulate version for dev environment
	SETUPTOOLS_SCM_PRETEND_VERSION="1.999.0dev0" uv pip install --system -r dev-requirements.in

.PHONY: fmt
fmt:
	pre-commit run ruff --all-files || true
	pre-commit run ruff-format --all-files || true

.PHONY: lint
lint: ## Run linters
	mypy flytekit/core
	mypy flytekit/types
#	allow-empty-bodies: Allow empty body in function.
#	disable-error-code="annotation-unchecked": Remove the warning "By default the bodies of untyped functions are not checked".
#	Mypy raises a warning because it cannot determine the type from the dataclass, despite we specified the type in the dataclass.
	mypy --allow-empty-bodies --disable-error-code="annotation-unchecked" tests/flytekit/unit/core
	pre-commit run --all-files

.PHONY: spellcheck
spellcheck:  ## Runs a spellchecker over all code and documentation
	# Configuration is in pyproject.toml
	codespell

.PHONY: test
test: lint unit_test

.PHONY: unit_test_codecov
unit_test_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" unit_test

.PHONY: unit_test_extras_codecov
unit_test_extras_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" unit_test_extras

.PHONY: unit_test
unit_test:
	# Skip all extra tests and run them with the necessary env var set so that a working (albeit slower)
	# library is used to serialize/deserialize protobufs is used.
	$(PYTEST_AND_OPTS) -m "not (serial or sandbox_test or hypothesis)" tests/flytekit/unit/ --ignore=tests/flytekit/unit/extras/ --ignore=tests/flytekit/unit/models --ignore=tests/flytekit/unit/extend ${CODECOV_OPTS}
	# Run serial tests without any parallelism
	$(PYTEST) -m "serial" tests/flytekit/unit/ --ignore=tests/flytekit/unit/extras/ --ignore=tests/flytekit/unit/models --ignore=tests/flytekit/unit/extend ${CODECOV_OPTS}

.PHONY: unit_test_hypothesis
unit_test_hypothesis:
	$(PYTEST_AND_OPTS) -m "hypothesis" tests/flytekit/unit/experimental ${CODECOV_OPTS}

.PHONY: unit_test_extras
unit_test_extras:
	PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python $(PYTEST_AND_OPTS) tests/flytekit/unit/extras tests/flytekit/unit/extend ${CODECOV_OPTS}

.PHONY: test_serialization_codecov
test_serialization_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" test_serialization

.PHONY: test_serialization
test_serialization:
	$(PYTEST_AND_OPTS) tests/flytekit/unit/models ${CODECOV_OPTS}


.PHONY: integration_test_codecov
integration_test_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" integration_test

.PHONY: integration_test
integration_test:
	$(PYTEST_AND_OPTS) tests/flytekit/integration --ignore=tests/flytekit/integration/remote/test_rust_remote.py  ${CODECOV_OPTS}

doc-requirements.txt: export CUSTOM_COMPILE_COMMAND := make doc-requirements.txt
doc-requirements.txt: doc-requirements.in install-piptools
	$(PIP_COMPILE) $<

${MOCK_FLYTE_REPO}/requirements.txt: export CUSTOM_COMPILE_COMMAND := make ${MOCK_FLYTE_REPO}/requirements.txt
${MOCK_FLYTE_REPO}/requirements.txt: ${MOCK_FLYTE_REPO}/requirements.in install-piptools
	$(PIP_COMPILE) $<

.PHONY: requirements
requirements: doc-requirements.txt ${MOCK_FLYTE_REPO}/requirements.txt ## Compile requirements

# TODO: Change this in the future to be all of flytekit
.PHONY: coverage
coverage:
	coverage run -m $(PYTEST) tests/flytekit/unit/core flytekit/types -m "not sandbox_test"
	coverage report -m --include="flytekit/core/*,flytekit/types/*"

.PHONY: build-dev
build-dev: export PLATFORM ?= linux/arm64
build-dev: export REGISTRY ?= localhost:30000
build-dev: export PYTHON_VERSION ?= 3.12
build-dev: export PSEUDO_VERSION ?= $(shell python -m setuptools_scm)
build-dev:
	docker build --platform ${PLATFORM} --push . -f Dockerfile.dev -t ${REGISTRY}/flytekit:${TAG} --build-arg PYTHON_VERSION=${PYTHON_VERSION} --build-arg PSEUDO_VERSION=${PSEUDO_VERSION}
