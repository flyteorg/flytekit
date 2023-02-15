from flytekit.clients.auth.default_html import get_default_success_html


def test_default_html():
    assert get_default_success_html("flyte.org") == """
<html>
    <head>
            <title>Oauth2 authentication Flow</title>
    </head>
    <body>
            <h1>Successfully logged into flyte.org</h1>
            <img src="https://artwork.lfaidata.foundation/projects/flyte/horizontal/color/flyte-horizontal-color.svg" alt="Flyte login"></img>
    </body>
</html>
"""  # noqa
