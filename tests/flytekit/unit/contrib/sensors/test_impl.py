from __future__ import absolute_import

import mock

from hmsclient import HMSClient
from hmsclient.genthrift.hive_metastore import ttypes as _ttypes

from flytekit.contrib.sensors.impl import HiveFilteredPartitionSensor
from flytekit.contrib.sensors.impl import HiveNamedPartitionSensor
from flytekit.contrib.sensors.impl import HiveTableSensor


def test_HiveTableSensor():
    hive_table_sensor = HiveTableSensor(
        table_name='mocked_table',
        host='localhost',
        port=1234,
    )
    assert hive_table_sensor._schema == 'default'
    with mock.patch.object(HMSClient, 'open'):
        with mock.patch.object(HMSClient, 'get_table'):
            success, interval = hive_table_sensor._do_poll()
            assert success
            assert interval is None

        with mock.patch.object(HMSClient, 'get_table', side_effect=_ttypes.NoSuchObjectException()):
            success, interval = hive_table_sensor._do_poll()
            assert not success
            assert interval is None


def test_HiveNamedPartitionSensor():
    hive_named_partition_sensor = HiveNamedPartitionSensor(
        table_name='mocked_table',
        partition_names=[
            'ds=2019-10-10',
            'ds=2019-10-11',
        ],
        host='localhost',
        port=1234,
    )
    assert hive_named_partition_sensor._schema == 'default'
    with mock.patch.object(HMSClient, 'open'):
        with mock.patch.object(HMSClient, 'get_partition_by_name'):
            success, interval = hive_named_partition_sensor._do_poll()
            assert success
            assert interval is None
        
        with mock.patch.object(HMSClient, 'get_partition_by_name', side_effect=_ttypes.NoSuchObjectException()):
            success, interval = hive_named_partition_sensor._do_poll()
            assert not success
            assert interval is None


def test_HiveFilteredPartitionSensor():
    hive_filtered_partition_sensor = HiveFilteredPartitionSensor(
        table_name='mocked_table',
        partition_filter="ds = '2019-10-10' AND region = 'NYC'",
        host='localhost',
        port=1234,
    )
    assert hive_filtered_partition_sensor._schema == 'default'
    with mock.patch.object(HMSClient, 'open'):
        with mock.patch.object(HMSClient, 'get_partitions_by_filter', return_value=['any']):
            success, interval = hive_filtered_partition_sensor._do_poll()
            assert success
            assert interval is None
        
        with mock.patch.object(HMSClient, 'get_partitions_by_filter', return_value=[]):
            success, interval = hive_filtered_partition_sensor._do_poll()
            assert not success
            assert interval is None
