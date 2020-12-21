


class FlyteDirectory(os.PathLike):
    """
    Please first read through the comments on the FlyteFile class as the implementation here is similar.

    One thing to note is that the os.PathLike type that comes with Python was used as a stand-in for FlyteFile.
    That is, if a task returns an os.PathLike, Flyte takes that to mean FlyteFile. There is no easy way to
    distinguish an os.PathLike where the user means a File and where the user means a Directory. As such, if you
    want to use a directory, you must declare all types as FlyteDirectory. You'll still be able to return a string
    literal though instead of a full-fledged FlyteDirectory object assuming the str is a directory.


    """

