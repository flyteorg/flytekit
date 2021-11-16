import os
from typing import Dict, List, NamedTuple, Tuple

import numpy as np
import pandas as pd
import xgboost
from flytekitplugins.xgboost import HyperParameters, ModelParameters, XGBoostParameters, XGBoostTrainerTask
from sklearn import model_selection

import flytekit
from flytekit import task, workflow
from flytekit.types.file import CSVFile, FlyteFile, JoblibSerializedFile
from flytekit.types.schema import FlyteSchema


def test_simple_model():
    xgboost_trainer = XGBoostTrainerTask(
        name="test1",
        config=XGBoostParameters(
            hyper_parameters=HyperParameters(max_depth=2, eta=1, objective="binary:logistic", verbosity=2),
            model_parameters=ModelParameters(num_boost_round=1),
        ),
        dataset_type=FlyteFile,
    )

    @workflow
    def train_test_wf(
        train: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train",
        test: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
    ) -> List[float]:
        _, predictions, _ = xgboost_trainer(
            train=train,
            test=test,
            params=XGBoostParameters(model_parameters=ModelParameters(num_boost_round=2)),
        )
        return predictions

    assert xgboost_trainer.python_interface.inputs == {
        "train": FlyteFile,
        "test": FlyteFile,
        "params": XGBoostParameters,
    }
    train_test_wf()


def test_csv_data():
    """
    Note: For CSV training, the algorithm assumes that the CSV does not have a header record.
    To set the target variable, set the label_column parameter, which by default is 0.
    """
    xgboost_trainer = XGBoostTrainerTask(
        name="test2",
        config=XGBoostParameters(label_column=0),
        dataset_type=CSVFile,
    )

    @task
    def partition_data(dataset: str) -> Tuple[CSVFile, CSVFile]:
        column_names = [
            "sex",
            "length",
            "diameter",
            "height",
            "whole weight",
            "shucked weight",
            "viscera weight",
            "shell weight",
            "rings",
        ]
        data = pd.read_csv(dataset, names=column_names)

        for label in "MFI":
            data[label] = data["sex"] == label
        del data["sex"]

        y = data.rings.values

        del data["rings"]  # remove rings from data, so we can convert all the dataframe to a numpy 2D array.
        X = data.values.astype(float)

        train_X, test_X, train_y, test_y = model_selection.train_test_split(
            X, y, test_size=0.33, random_state=42
        )  # splits 75%/25% by default

        X_combined = np.concatenate((train_y[:, None], train_X), axis=1)
        y_combined = np.concatenate((test_y[:, None], test_X), axis=1)

        working_dir = flytekit.current_context().working_directory

        train_path = os.path.join(working_dir, "train.csv")
        test_path = os.path.join(working_dir, "test.csv")

        pd.DataFrame(X_combined).to_csv(train_path, index=False, header=False)
        pd.DataFrame(y_combined).to_csv(test_path, index=False, header=False)

        return train_path, test_path

    wf_output = NamedTuple(
        "wf_output",
        model=JoblibSerializedFile,
        predictions=List[float],
        evaluation_result=Dict[str, Dict[str, List[float]]],
    )

    @workflow
    def wf() -> wf_output:
        train_data, test_data = partition_data(dataset="abalone.data")
        return xgboost_trainer(
            train=train_data,
            test=test_data,
            params=XGBoostParameters(
                hyper_parameters=HyperParameters(
                    objective="reg:linear",
                    eta=0.2,
                    gamma=4,
                    max_depth=5,
                    subsample=0.7,
                    verbosity=0,
                    min_child_weight=6,
                )
            ),
        )

    assert xgboost_trainer.python_interface.inputs == {
        "train": CSVFile,
        "test": CSVFile,
        "params": XGBoostParameters,
    }
    assert xgboost_trainer._config.label_column == 0
    wf()


def test_local_data():
    xgboost_trainer = XGBoostTrainerTask(
        name="test3",
        config=XGBoostParameters(
            hyper_parameters=HyperParameters(
                objective="reg:linear",
                eta=0.2,
                gamma=4,
                max_depth=5,
                subsample=0.7,
                verbosity=0,
            ),
            label_column=0,
        ),
        dataset_type=CSVFile,
    )

    @workflow
    def wf(
        train_data: CSVFile = "abalone_train.csv",
        test_data: CSVFile = "abalone_test.csv",
    ) -> List[float]:
        _, predictions, _ = xgboost_trainer(
            train=train_data,
            test=test_data,
            params=XGBoostParameters(hyper_parameters=HyperParameters(min_child_weight=6)),
        )
        return predictions

    assert xgboost_trainer.python_interface.inputs == {
        "train": CSVFile,
        "test": CSVFile,
        "params": XGBoostParameters,
    }
    wf()


def test_schema_data():
    """
    Note: To set the target variable, set the label_column parameter, which by default is 0.
    """
    xgboost_trainer = XGBoostTrainerTask(
        name="test4",
        config=XGBoostParameters(
            hyper_parameters=HyperParameters(
                objective="reg:linear",
                eta=0.2,
                gamma=4,
                max_depth=5,
                subsample=0.7,
                verbosity=0,
            ),
            label_column=0,
        ),
        dataset_type=FlyteSchema,
    )

    @task
    def csv_to_df(data: str) -> pd.DataFrame:
        return pd.read_csv(data)

    @workflow
    def wf(train_data: str = "abalone_train.csv", test_data: str = "abalone_test.csv") -> List[float]:
        _, predictions, _ = xgboost_trainer(
            train=csv_to_df(data=train_data),
            test=csv_to_df(data=test_data),
            params=XGBoostParameters(),
        )
        return predictions

    assert xgboost_trainer.python_interface.inputs == {
        "train": FlyteSchema,
        "test": FlyteSchema,
        "params": XGBoostParameters,
    }
    wf()


def test_pipeline():
    xgboost_trainer = XGBoostTrainerTask(
        name="test5",
        config=XGBoostParameters(
            hyper_parameters=HyperParameters(max_depth=2, eta=1, objective="binary:logistic", verbosity=2)
        ),
        dataset_type=FlyteFile,
        validate=True,
    )

    @task
    def estimate_accuracy(predictions: List[float], test: FlyteFile) -> float:
        test.download()
        dtest = xgboost.DMatrix(test.path)
        labels = dtest.get_label()
        return sum(1 for i in range(len(predictions)) if int(predictions[i] > 0.5) == labels[i]) / float(
            len(predictions)
        )

    wf_output = NamedTuple(
        "wf_output",
        model=JoblibSerializedFile,
        accuracy=float,
        evaluation_result=Dict[str, Dict[str, List[float]]],
    )

    @workflow
    def full_pipeline(
        train: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train",
        test: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
        validation: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
    ) -> wf_output:
        model, predictions, evaluation_result = xgboost_trainer(
            train=train,
            test=test,
            validation=validation,
            params=XGBoostParameters(model_parameters=ModelParameters(num_boost_round=2)),
        )
        return (
            model,
            estimate_accuracy(predictions=predictions, test=test),
            evaluation_result,
        )

    assert xgboost_trainer.python_interface.inputs == {
        "train": FlyteFile,
        "test": FlyteFile,
        "validation": FlyteFile,
        "params": XGBoostParameters,
    }
    assert full_pipeline().accuracy >= 0.7
