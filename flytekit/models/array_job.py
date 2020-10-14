import json as _json

from flyteidl.plugins import array_job_pb2 as _array_job
from google.protobuf import json_format as _json_format

from flytekit.models import common as _common


class ArrayJob(_common.FlyteCustomIdlEntity):
    def __init__(self, parallelism, size, min_successes):
        """
        Initializes a new ArrayJob.
        :param int parallelism: Defines the minimum number of instances to bring up concurrently at any given point.
        :param int size: Defines the number of instances to launch at most. This number should match the size of
            the input if the job requires processing of all input data. This has to be a positive number.
        :param int min_successes: An absolute number of the minimum number of successful completions of subtasks. As
            soon as this criteria is met, the array job will be marked as successful and outputs will be computed.
        """
        self._parallelism = parallelism
        self._size = size
        self._min_successes = min_successes

    @property
    def parallelism(self):
        """
        Defines the minimum number of instances to bring up concurrently at any given point.

        :rtype: int
        """
        return self._parallelism

    @property
    def size(self):
        """
         Defines the number of instances to launch at most. This number should match the size of the input if the job
         requires processing of all input data. This has to be a positive number.

        :rtype: int
        """
        return self._size

    @size.setter
    def size(self, value):
        self._size = value

    @property
    def min_successes(self):
        """
        An absolute number of the minimum number of successful completions of subtasks. As soon as this criteria is met,
            the array job will be marked as successful and outputs will be computed.

        :rtype: int
        """
        return self._min_successes

    @min_successes.setter
    def min_successes(self, value):
        self._min_successes = value

    def to_dict(self):
        """
        :rtype: dict[T, Text]
        """
        return _json_format.MessageToDict(
            _array_job.ArrayJob(parallelism=self.parallelism, size=self.size, min_successes=self.min_successes,)
        )

    @classmethod
    def from_dict(cls, idl_dict):
        """
        :param dict[T, Text] idl_dict:
        :rtype: ArrayJob
        """
        pb2_object = _json_format.Parse(_json.dumps(idl_dict), _array_job.ArrayJob())

        return cls(parallelism=pb2_object.parallelism, size=pb2_object.size, min_successes=pb2_object.min_successes,)
