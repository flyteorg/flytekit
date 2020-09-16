from flytekit.common import constants as _common_constants


# The default output-persisting predicate.
# With this predicate, only the copy running on the first host in the list of hosts would persist its output
class DefaultOutputPersistPredicate(object):
    def __call__(self, distributed_training_context):
        return (
                distributed_training_context[_common_constants.DistributedTrainingContextKey.CURRENT_HOST]
                == distributed_training_context[_common_constants.DistributedTrainingContextKey.HOSTS][0]
        )
