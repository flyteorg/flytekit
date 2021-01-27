.PHONY: test
test:
	pytest tests

.PHONY: install-all-dev
install-all-dev:
	pip install -e .

.PHONY: build_all_plugins
build_all_plugins:
	run_all_plugins.sh python setup.py sdist bdist_wheel

