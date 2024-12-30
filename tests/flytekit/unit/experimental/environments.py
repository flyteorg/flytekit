from flytekit.core.environments import Environment, inherit


def test_basic_environment():
    
    env = Environment(cache=False)
    
    @env.task
    def foo():
        pass

    @env
    def bar():
        pass

def test_extended_environment():
    
    env = Environment(cache=False)
    
    other = env.extend(cache=True)
    
    @other.task
    def foo():
        pass

    @other
    def bar():
        pass

def test_updated_environment():
    
    env = Environment(cache=False)
    
    env.update(cache=True)
    
    @env.task
    def foo():
        pass

    @env
    def bar():
        pass

def test_show_environment():
    
    env = Environment(cache=False)
    
    env.show()

def test_inherit():
    
    old_config = {"cache": False, "timeout": 10}
    
    new_config = {"cache": True}
    
    combined = inherit(old_config, new_config)
    
    assert combined["cache"] == True
    assert combined["timeout"] == 10