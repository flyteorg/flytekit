from flytekit.core.environment import Environment, inherit

def test_basic_environment():

    env = Environment(retries=2)

    @env.task
    def foo():
        pass

    @env
    def bar():
        pass

    assert foo._metadata.retries == 2
    assert bar._metadata.retries == 2

def test_extended_environment():

    env = Environment(retries=2)

    other = env.extend(retries=0)

    @other.task
    def foo():
        pass

    @other
    def bar():
        pass


    assert foo._metadata.retries == 0
    assert bar._metadata.retries == 0

def test_updated_environment():

    env = Environment(retries=2)

    env.update(retries=0)

    @env.task
    def foo():
        pass

    @env
    def bar():
        pass


    assert foo._metadata.retries == 0
    assert bar._metadata.retries == 0

def test_show_environment():

    env = Environment(retries=2)

    env.show()

def test_inherit():

    old_config = {"cache": False, "timeout": 10}

    new_config = {"cache": True}

    combined = inherit(old_config, new_config)

    assert combined["cache"] == True
    assert combined["timeout"] == 10
