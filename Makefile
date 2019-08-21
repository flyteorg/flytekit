.PHONY: build_python
build_python:
	@python setup.py sdist

.PHONY: package
package:
	@package.sh

unit_python2:
	PYSPARK_PYTHON=python PYSPARK_DRIVER_PYTHON=python SPARK_YARN_USER_ENV="PYSPARK_PYTHON=python" python -m pytest tests/flytekit/unit

unit_python3:
	PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 SPARK_YARN_USER_ENV="PYSPARK_PYTHON=python3" python3 -m pytest tests/flytekit/unit

.PHONY: release
release:
	./package.sh
