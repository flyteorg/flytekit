.PHONY: test
test:
	pytest pod/tests
	pytest spark/tests
	pytest papermill/tests
	pytest kfpytorch/tests
	pytest aws/tests
	pytest kftensorflow/tests

.PHONY: install-all-dev
install-all-dev:
	pip install -e .
