from typing import Annotated, List, NamedTuple, TypeVar

import numpy
import onnxruntime as rt
import pandas as pd
from flytekitplugins.onnx_scikitlearn import ScikitLearn2ONNX, ScikitLearn2ONNXConfig
from skl2onnx.common.data_types import FloatTensorType
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from flytekit import task, workflow
from flytekit.types.file.file import FlyteFile


def test_scikitlearn_simple():
    TrainOutput = NamedTuple(
        "TrainOutput",
        [
            (
                "model",
                Annotated[
                    ScikitLearn2ONNX,
                    ScikitLearn2ONNXConfig(
                        initial_types=[("float_input", FloatTensorType([None, 4]))],
                        target_opset=12,
                    ),
                ],
            ),
            ("test", pd.DataFrame),
        ],
    )

    @task
    def train() -> TrainOutput:
        iris = load_iris(as_frame=True)
        X, y = iris.data, iris.target
        X_train, X_test, y_train, _ = train_test_split(X, y)
        model = RandomForestClassifier()
        model.fit(X_train, y_train)

        return TrainOutput(test=X_test, model=ScikitLearn2ONNX(model))

    @task
    def predict(
        model: FlyteFile[TypeVar("onnx")],
        X_test: pd.DataFrame,
    ) -> List[int]:
        sess = rt.InferenceSession(model.download())
        input_name = sess.get_inputs()[0].name
        label_name = sess.get_outputs()[0].name
        pred_onx = sess.run([label_name], {input_name: X_test.to_numpy(dtype=numpy.float32)})[0]
        return pred_onx.tolist()

    @workflow
    def wf() -> List[int]:
        train_output = train()
        return predict(model=train_output.model, X_test=train_output.test)

    print(wf())
