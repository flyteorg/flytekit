import sys

# tensorflow does not ship python 3.14 wheels yet, see dev-requirements.in
if sys.version_info >= (3, 14):
    collect_ignore_glob = ["*"]
