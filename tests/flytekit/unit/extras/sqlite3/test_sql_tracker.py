from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from tests.flytekit.unit.extras.sqlite3.test_task import tk as not_tk


def test_sql_lhs():
    assert not_tk.lhs == "tk"


def test_sql_command():
    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    srz_t = get_serializable(serialization_settings, not_tk)
    assert srz_t.container.args[-8:] == [
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--resolver-args",
        "--",
        "task-module",
        "tests.flytekit.unit.extras.sqlite3.test_task",
        "task-name",
        "tk",
    ]
