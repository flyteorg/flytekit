import numpy as np

from flytekit import task, workflow


@task
def generate_numpy_1d() -> np.ndarray:
    return np.array([1, 2, 3, 4, 5, 6], dtype=int)


@task
def generate_numpy_2d() -> np.ndarray:
    return np.array([[1.8, 2.9, 3.1], [5.4, 6.0, 7.7]])


@task
def generate_numpy_dtype_object() -> np.ndarray:
    # dtype=object cannot be serialized
    return np.array(
        [
            [
                405,
                162,
                414,
                0,
                np.array([list([1, 9, 2]), 18, (405, 18, 207), 64, "Universal"], dtype=object),
                0,
                0,
                0,
            ]
        ],
        dtype=object,
    )


@task
def t1(array: np.ndarray) -> np.ndarray:
    assert array.dtype == int
    output = np.empty(len(array))
    for i in range(len(array)):
        output[i] = 1.0 / array[i]
    return output


@task
def t2(array: np.ndarray) -> np.ndarray:
    return array.flatten()


@task
def t3(array: np.ndarray) -> np.ndarray:
    # convert 1D numpy array to 3D
    return array.reshape(2, 3)


@workflow
def wf():
    array_1d = generate_numpy_1d()
    array_2d = generate_numpy_2d()
    try:
        generate_numpy_dtype_object()
    except Exception as e:
        assert isinstance(e, TypeError)
    t1(array=array_1d)
    t2(array=array_2d)
    t3(array=array_1d)


@workflow
def test_wf():
    wf()
