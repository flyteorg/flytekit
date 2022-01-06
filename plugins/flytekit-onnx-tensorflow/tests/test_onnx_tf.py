from typing import Tuple

import joblib
import numpy as np
import onnxruntime as rt
import tensorflow as tf
from flytekitplugins.onnx_tensorflow import TensorFlow2ONNX, TensorFlow2ONNXConfig
from tensorflow.keras.applications.resnet50 import ResNet50, preprocess_input
from tensorflow.keras.preprocessing import image

from flytekit import task, workflow
from flytekit.types.file import JoblibSerializedFile


def test_tf_simple():
    @task
    def load_test_img() -> np.ndarray:
        img_path = "ade20k.jpg"

        img = image.load_img(img_path, target_size=(224, 224))

        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        x = preprocess_input(x)
        return x

    @task
    def train_and_predict(img: np.ndarray) -> Tuple[JoblibSerializedFile, np.ndarray]:
        model = ResNet50(weights="imagenet")

        preds = model.predict(img)

        # serialize model using joblib
        jbfile = "model.joblib.dat"
        joblib.dump(model, jbfile)
        return jbfile, preds

    @task
    def onnx_predict(
        model: TensorFlow2ONNX[
            TensorFlow2ONNXConfig(
                input_signature=(tf.TensorSpec((None, 224, 224, 3), tf.float32, name="input"),), opset=13
            )
        ],
        img: np.ndarray,
    ) -> np.ndarray:
        m = rt.InferenceSession(model.onnx.download(), providers=["CPUExecutionProvider"])
        onnx_pred = m.run([n.name for n in m.get_outputs()], {"input": img})

        return onnx_pred

    @workflow
    def wf() -> Tuple[np.ndarray, np.ndarray]:
        img = load_test_img()
        model, keras_preds = train_and_predict(img=img)
        onnx_preds = onnx_predict(model=model, img=img)
        return keras_preds, onnx_preds

    keras_preds, onnx_preds = wf()
    np.testing.assert_allclose(keras_preds, onnx_preds[0], rtol=1e-5)
