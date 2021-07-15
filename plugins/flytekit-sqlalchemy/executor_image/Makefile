PIP_COMPILE = pip-compile --upgrade --verbose

.PHONY: install-piptools
install-piptools:
	pip install -U pip-tools

requirements.txt: export CUSTOM_COMPILE_COMMAND := make requirements.txt
requirements.txt: requirements.in install-piptools
	$(PIP_COMPILE) $<
