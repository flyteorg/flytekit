

class A(object):
    ...


class AA(A):
    def __init__(self):
        self._hello = "world"

    @property
    def hello(self) -> str:
        return self._hello


class BB(A):

    def __init__(self, a: AA):
        self._aa = a

    @property
    def b_prop(self) -> str:
        return "hello"

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            return getattr(object.__getattribute__(self, "_aa"), item)


def test_fds():
    a = AA()
    b = BB(a=a)
    print(b.b_prop)
    print(b.hello)

