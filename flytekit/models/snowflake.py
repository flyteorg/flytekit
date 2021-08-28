from flyteidl.plugins import snowflake_pb2 as _snowflake

from flytekit.models import common as _common


class SnowflakeQuery(_common.FlyteIdlEntity):
    def __init__(self, account=None, warehouse=None, schema=None, database=None):
        """
        Initializes a new SnowflakeQuery.

        :param string account:
        :param string warehouse:
        :param string schema:
        :param string database:

        """
        self._account = account
        self._warehouse = warehouse
        self._schema = schema
        self._database = database

    @property
    def account(self):
        """
        The snowflake account.
        :rtype: str
        """
        return self._account

    @property
    def warehouse(self):
        """
        :rtype: str
        """
        return self._warehouse

    @property
    def schema(self):
        """
        :rtype: str
        """
        return self._schema

    @property
    def database(self):
        """
        :rtype: str
        """
        return self._database

    def to_flyte_idl(self):
        """
        :rtype: _snowflake.SnowflakeQuery
        """
        return _snowflake.SnowflakeQuery(
            account=self._account,
            warehouse=self._warehouse,
            schema=self._schema,
            database=self._database,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _snowflake.SnowflakeQuery pb2_object:
        :return: SnowflakeQuery
        """
        return cls(
            account=pb2_object.account,
            warehouse=pb2_object.warehouse,
            schema=pb2_object.schema,
            database=pb2_object.database,
        )
