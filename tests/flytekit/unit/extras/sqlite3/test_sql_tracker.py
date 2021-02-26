from tests.flytekit.unit.extras.sqlite3.test_task import tk as not_tk


def test_sql_lhs():
    assert not_tk.lhs == "tk"
