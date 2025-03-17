export REPOSITORY=flytekit

PIP_COMPILE = pip-compile --upgrade --verbose --resolver=backtracking
MOCK_FLYTE_REPO=tests/flytekit/integration/remote/mock_flyte_repo/workflows
PYTEST_OPTS ?= -n auto --dist=loadfile
PYTEST_AND_OPTS = pytest ${PYTEST_OPTS}
PYTEST = pytest
PYTHON_VERSION ?= 3.9
UV_EXTRAS_WITH ?=
UV_RUN = uv run --python=${PYTHON_VERSION} --with ".[base-dev]" ${UV_EXTRAS_WITH}

.SILENT: help
.PHONY: help
help:
	echo Available recipes:
	cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' | awk 'BEGIN { FS = ":.*?## " } { cnt++; a[cnt] = $$1; b[cnt] = $$2; if (length($$1) > max) max = length($$1) } END { for (i = 1; i <= cnt; i++) printf "  $(shell tput setaf 6)%-*s$(shell tput setaf 0) %s\n", max, a[i], b[i] }'
	tput sgr0

.PHONY: fmt
fmt:
	$(UV_RUN) pre-commit run ruff --all-files || true
	$(UV_RUN) pre-commit run ruff-format --all-files || true

.PHONY: lint
lint: ## Run linters
	$(UV_RUN) mypy flytekit/core
	$(UV_RUN) mypy flytekit/types
#	allow-empty-bodies: Allow empty body in function.
#	disable-error-code="annotation-unchecked": Remove the warning "By default the bodies of untyped functions are not checked".
#	Mypy raises a warning because it cannot determine the type from the dataclass, despite we specified the type in the dataclass.
	$(UV_RUN) mypy --allow-empty-bodies --disable-error-code="annotation-unchecked" tests/flytekit/unit/core
	$(UV_RUN) pre-commit run --all-files

.PHONY: spellcheck
spellcheck:  ## Runs a spellchecker over all code and documentation
	# Configuration is in pyproject.toml
	$(UV_RUN) codespell

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
	$(UV_RUN) $(PYTEST_AND_OPTS) -m "not (serial or sandbox_test or hypothesis)" tests/flytekit/unit/ --ignore=tests/flytekit/unit/extras/ --ignore=tests/flytekit/unit/models --ignore=tests/flytekit/unit/extend ${CODECOV_OPTS}
	# Run serial tests without any parallelism
	$(UV_RUN) $(PYTEST) -m "serial" tests/flytekit/unit/ --ignore=tests/flytekit/unit/extras/ --ignore=tests/flytekit/unit/models --ignore=tests/flytekit/unit/extend ${CODECOV_OPTS}

.PHONY: unit_test_extras
unit_test_extras:
	PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python $(UV_RUN) $(PYTEST_AND_OPTS) tests/flytekit/unit/extras tests/flytekit/unit/extend ${CODECOV_OPTS}

.PHONY: test_serialization_codecov
test_serialization_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" test_serialization

.PHONY: test_serialization
test_serialization:
	$(UV_RUN) $(PYTEST_AND_OPTS) tests/flytekit/unit/models ${CODECOV_OPTS}

.PHONY: integration_test_codecov
integration_test_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" integration_test

.PHONY: integration_test
integration_test:
	$(UV_RUN) $(PYTEST_AND_OPTS) tests/flytekit/integration ${CODECOV_OPTS} -m "not lftransfers"

.PHONY: integration_test_lftransfers_codecov
integration_test_lftransfers_codecov:
	$(MAKE) CODECOV_OPTS="--cov=./ --cov-report=xml --cov-append" integration_test_lftransfers

.PHONY: integration_test_lftransfers
integration_test_lftransfers:
	$(UV_RUN) $(PYTEST) tests/flytekit/integration ${CODECOV_OPTS} -m "lftransfers"

.PHONY: build-dev
build-dev: export PLATFORM ?= linux/arm64
build-dev: export REGISTRY ?= localhost:30000
build-dev: export PSEUDO_VERSION ?= $(shell uv run --python=$(PYTHON_VERSION) --with setuptools_scm python -m setuptools_scm)
build-dev: export TAG ?= dev
build-dev:
	docker build --platform ${PLATFORM} --push . -f Dockerfile.dev -t ${REGISTRY}/flytekit:${TAG} --build-arg PYTHON_VERSION=${PYTHON_VERSION} --build-arg PSEUDO_VERSION=${PSEUDO_VERSION}
