from flytekit.contrib.sensors.base_sensor import Sensor as _Sensor
from flytekit.plugins import hmsclient as _hmsclient


class _HiveSensor(_Sensor):
    def __init__(self, host, port, schema="default", **kwargs):
        """
        :param Text host:
        :param Text port:
        :param Text schema: The schema/database that we should consider.
        :param **kwargs: See flytekit.contrib.sensors.base_sensor.Sensor for more
            parameters.
        """
        self._schema = schema
        self._host = host
        self._port = port
        self._hive_metastore_client = _hmsclient.HMSClient(host=host, port=port)
        super(_HiveSensor, self).__init__(**kwargs)


class HiveTableSensor(_HiveSensor):
    def __init__(self, table_name, host, port, **kwargs):
        """
        :param Text host: The host for the Hive metastore Thrift service.
        :param Text port: The port for the Hive metastore Thrift Service
        :param Text table_name: The name of the table to look for.
        :param **kwargs: See _HiveSensor and flytekit.contrib.sensors.base_sensor.Sensor for more
            parameters.
        """
        super(HiveTableSensor, self).__init__(host, port, **kwargs)
        self._table_name = table_name

    def _do_poll(self):
        """
        :rtype: (bool, Optional[datetime.timedelta])
        """
        with self._hive_metastore_client as client:
            try:
                client.get_table(self._schema, self._table_name)
                return True, None
            except _hmsclient.genthrift.hive_metastore.ttypes.NoSuchObjectException:
                return False, None


class HiveNamedPartitionSensor(_HiveSensor):
    def __init__(self, table_name, partition_names, host, port, **kwargs):
        """
        This class allows sensing for a specific named Hive Partition.  This is the preferred partition sensing
        operator because it is more efficient than evaluating a filter expression.

        :param Text table_name: The name of the table
        :param Text partition_name: The name of the partition to listen for  (example: 'ds=2017-01-01/region=NYC')
        :param Text host: The host for the Hive metastore Thrift service.
        :param Text port: The port for the Hive metastore Thrift Service
        :param **kwargs: See _HiveSensor and flytekit.contrib.sensors.base_sensor.Sensor for more
            parameters.
        """
        super(HiveNamedPartitionSensor, self).__init__(host, port, **kwargs)
        self._table_name = table_name
        self._partition_names = partition_names

    def _do_poll(self):
        """
        :rtype: (bool, Optional[datetime.timedelta])
        """
        with self._hive_metastore_client as client:
            try:
                for partition_name in self._partition_names:
                    client.get_partition_by_name(self._schema, self._table_name, partition_name)
                return True, None
            except _hmsclient.genthrift.hive_metastore.ttypes.NoSuchObjectException:
                return False, None


class HiveFilteredPartitionSensor(_HiveSensor):
    def __init__(self, table_name, partition_filter, host, port, **kwargs):
        """
        This class allows sensing for any Hive partition that matches a filter expression.  It is recommended that the
        user should use HiveNamedPartitionSensor instead when possible because it is a more efficient API.

        :param Text table_name: The name of the table
        :param Text partition_filter: A filter expression for the partition.  (example: "ds = '2017-01-01' and
            region='NYC')
        :param Text host: The host for the Hive metastore Thrift service.
        :param Text port: The port for the Hive metastore Thrift Service
        :param **kwargs: See _HiveSensor and flytekit.contrib.sensors.base_sensor.Sensor for more
            parameters.
        """
        super(HiveFilteredPartitionSensor, self).__init__(host, port, **kwargs)
        self._table_name = table_name
        self._partition_filter = partition_filter

    def _do_poll(self):
        """
        :rtype: (bool, Optional[datetime.timedelta])
        """
        with self._hive_metastore_client as client:
            partitions = client.get_partitions_by_filter(
                db_name=self._schema, tbl_name=self._table_name, filter=self._partition_filter, max_parts=1,
            )
            if partitions:
                return True, None
            else:
                return False, None
