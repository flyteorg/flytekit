.PHONY: test
test:
	pytest plugins/pod/tests
	pytest plugins/spark/tests
	pytest plugins/papermill/tests
	pytest plugins/kfpytorch/tests
	pytest plugins/sagemaker/tests

.PHONY: install-all-dev
install-all-dev:
	pip install -e .
