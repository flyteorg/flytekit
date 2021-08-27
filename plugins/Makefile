.PHONY: test
test:
	pytest tests

.PHONY: install-all-dev
install-all-dev:
	pip install -e .

.PHONY: build_all_plugins
build_all_plugins:
	./run_all_plugins.sh python setup.py sdist bdist_wheel

.PHONY: publish_all_plugins
publish_all_plugins:
	twine upload */dist/*

PLACEHOLDER := "__version__\ =\ \"0.0.0+develop\""
VERSION_FILE := "setup.py"

.PHONY: update_all_versions
update_all_versions:
	# ensure the placeholder is there. If grep doesn't find the placeholder
	# it exits with exit code 1 and github actions aborts the build.
	./run_all_plugins.sh grep "$(PLACEHOLDER)" "$(VERSION_FILE)"
	./run_all_plugins.sh sed -i "s/$(PLACEHOLDER)/__version__ = \"${VERSION}\"/g" $(VERSION_FILE)
