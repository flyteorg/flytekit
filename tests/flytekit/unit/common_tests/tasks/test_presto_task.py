from datetime import timedelta as _timedelta

from flytekit.common.tasks.presto_task import SdkPrestoTask


def test_abc():
    from flytekit.models.qubole import QuboleHiveJob, HiveQuery
    # This should change ofc to be the plugin idl object for Presto
    q = QuboleHiveJob(HiveQuery(query="select * from test", timeout_sec=10, retry_count=1), cluster_label="default",
                      tags=["abc"])

    j = SdkPrestoTask(hive_job=q.to_flyte_idl(), discoverable=False, discovery_version=None,
                      retries=2, timeout=_timedelta(days=1), cluster_label="default", tags=[], environment={})
    print(j)
