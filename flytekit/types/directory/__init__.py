"""
Similar to {{< py_class_ref flytekit.types.file.FlyteFile >}} there are some 'preformatted' directory types.

| Class | Description |
|-------| ---- |
| {{< py_class_ref FlyteDirectory >}} | {{< py_class_docsum FlyteDirectory >}} |
| {{< py_class_ref TensorboardLogs >}} | {{< py_class_docsum TensorboardLogs >}} |
| {{< py_class_ref TFRecordsDirectory >}} | {{< py_class_docsum TFRecordsDirectory >}} |

"""

import typing

from .types import FlyteDirectory, FlyteDirToMultipartBlobTransformer

# The following section provides some predefined aliases for commonly used FlyteDirectory formats.

tensorboard = typing.TypeVar("tensorboard")
TensorboardLogs = FlyteDirectory[tensorboard]
"""
    This type can be used to denote that the output is a folder that contains logs that can be loaded in TensorBoard.
    This is usually the SummaryWriter output in PyTorch or Keras callbacks which record the history readable by
    TensorBoard.
"""

tfrecords_dir = typing.TypeVar("tfrecords_dir")
TFRecordsDirectory = FlyteDirectory[tfrecords_dir]
"""
    This type can be used to denote that the output is a folder that contains tensorflow record files.
    This is usually the TFRecordWriter output in Tensorflow which writes serialised tf.train.Example
    message (or protobuf) to tfrecord files
"""
