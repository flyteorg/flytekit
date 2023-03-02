class AccessTokenNotFoundError(RuntimeError):
    """
    This error is raised with Access token is not found or if Refreshing the token fails
    """

    pass


class AuthenticationError(RuntimeError):
    """
    This is raised for any AuthenticationError
    """

    pass
