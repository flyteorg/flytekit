import http.server as _BaseHTTPServer
import re
from multiprocessing import Queue as _Queue

from flytekit.clis.auth import auth as _auth


def test_generate_code_verifier():
    verifier = _auth._generate_code_verifier()
    assert verifier is not None
    assert 43 < len(verifier) < 128
    assert not re.search(r"[^a-zA-Z0-9_\-.~]+", verifier)


def test_generate_state_parameter():
    param = _auth._generate_state_parameter()
    assert not re.search(r"[^a-zA-Z0-9-_.,]+", param)


def test_create_code_challenge():
    test_code_verifier = "test_code_verifier"
    assert _auth._create_code_challenge(test_code_verifier) == "Qq1fGD0HhxwbmeMrqaebgn1qhvKeguQPXqLdpmixaM4"


def test_oauth_http_server():
    queue = _Queue()
    server = _auth.OAuthHTTPServer(("localhost", 9000), _BaseHTTPServer.BaseHTTPRequestHandler, queue=queue)
    test_auth_code = "auth_code"
    server.handle_authorization_code(test_auth_code)
    auth_code = queue.get()
    assert test_auth_code == auth_code
