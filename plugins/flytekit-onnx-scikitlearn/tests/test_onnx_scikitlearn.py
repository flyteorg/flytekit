from typing import List, Tuple

import joblib
import numpy
import onnxruntime as rt
import pandas as pd
from flytekitplugins.onnx_scikitlearn import ScikitLearn2ONNX, ScikitLearn2ONNXConfig
from skl2onnx.common.data_types import FloatTensorType
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from flytekit import task, workflow
from flytekit.types.file import JoblibSerializedFile


def test_scikitlearn_simple():
    @task
    def train() -> Tuple[pd.DataFrame, JoblibSerializedFile]:
        iris = load_iris(as_frame=True)
        X, y = iris.data, iris.target
        X_train, X_test, y_train, _ = train_test_split(X, y)
        model = RandomForestClassifier()
        model.fit(X_train, y_train)

        # serialize model using joblib
        jbfile = "model.joblib.dat"
        joblib.dump(model, jbfile)

        return X_test, jbfile

    @task
    def predict(
        model: ScikitLearn2ONNX[
            ScikitLearn2ONNXConfig(
                initial_types=[("float_input", FloatTensorType([None, 4]))],
                target_opset=12,
            )
        ],
        X_test: pd.DataFrame,
    ) -> List[int]:
        # model.model and model.onnx should be accessible
        sess = rt.InferenceSession(model.onnx.download())
        input_name = sess.get_inputs()[0].name
        label_name = sess.get_outputs()[0].name
        pred_onx = sess.run([label_name], {input_name: X_test.to_numpy(dtype=numpy.float32)})[0]
        return pred_onx.tolist()

    @workflow
    def wf() -> List[int]:
        X_test, jbfile = train()
        return predict(model=jbfile, X_test=X_test)

    print(wf())
