from __future__ import absolute_import

from flytekit.clis.auth import auth as _auth


def test_generate_code_verifier():
    verifier = _auth._generate_code_verifier()
    # TODO: Write test later
    assert verifier is not None
