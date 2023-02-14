def get_default_success_html(endpoint: str) -> str:
    return f"""
<html>
        <head>
                <title>Oauth2 authentication Flow</title>
        </head>
        <body>
                <h1>Log in successful to {endpoint}</h1>
                <img src="https://artwork.lfaidata.foundation/projects/flyte/horizontal/color/flyte-horizontal-color.svg" alt="Flyte login"></img>
        </body>
</html>
"""
