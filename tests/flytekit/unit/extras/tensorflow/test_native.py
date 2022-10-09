import tensorflow

from flytekit import task, workflow


@task
def generate_tensor_1d() -> tensorflow.Tensor:
    return tensorflow.zeros(5, dtype=tensorflow.int32)


@task
def generate_tensor_2d() -> tensorflow.Tensor:
    return tensorflow.constant([[1.0, -1.0, 2], [1.0, -1.0, 9], [0, 7.0, 3]])


class MyModel(tensorflow.Module):
    def __init__(self):
        super(MyModel, self).__init__()
        self.l0 = tensorflow.keras.layers.Dense(2, input_shape=(4,), activation=None)
        self.l1 = tensorflow.keras.layers.Dense(1, input_shape=(2,), activation=None)

    def forward(self, input):
        out0 = self.l0(input)
        out0_relu = tensorflow.nn.relu(out0)
        return self.l1(out0_relu)


@task
def generate_model() -> tensorflow.Module:
    return MyModel()


@task
def t1(tensor: tensorflow.Tensor) -> tensorflow.Tensor:
    assert tensor.dtype == tensorflow.int32
    return tensor


@task
def t2(tensor: tensorflow.Tensor) -> tensorflow.Tensor:
    # convert 2D to 3D
    return tensorflow.expand_dims(tensor, 2)  # shape: [3, 3, 1]


@task
def t3(model: tensorflow.Module) -> tensorflow.Tensor:
    return model.weight


@task
def t4(model: tensorflow.Module) -> tensorflow.Module:
    return model.l1


@workflow
def wf():
    t1(tensor=generate_tensor_1d())
    t2(tensor=generate_tensor_2d())
    t4(model=MyModel())


@workflow
def test_wf():
    wf()
