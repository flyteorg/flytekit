import os
from dataclasses import asdict, dataclass
from typing import Dict, List, NamedTuple, Tuple

import numpy as np
import pandas as pd
import xgboost
from dataclasses_json import dataclass_json
from flytekitplugins.xgboost import TrainParameters, XGBoostTask
from sklearn import model_selection

import flytekit
from flytekit import kwtypes, task, workflow
from flytekit.types.file import CSVFile, FlyteFile, JoblibSerializedFile
from flytekit.types.schema import FlyteSchema


@dataclass_json
@dataclass
class Hyperparameters:
    max_depth: int = 2
    eta: int = 1
    objective: str = "binary:logistic"
    verbosity: int = 2


@dataclass_json
@dataclass
class CSVHyperparameters:
    objective: str = "reg:linear"
    eta: float = 0.2
    gamma: int = 4
    max_depth: int = 5
    subsample: float = 0.7
    silent: int = 0
    min_child_weight: int = 6


def test_simple_model():
    xgboost_trainer = XGBoostTask(
        name="test1",
        hyperparameters=asdict(Hyperparameters()),
        task_config=TrainParameters(num_boost_round=2),
        inputs=kwtypes(train=FlyteFile, test=FlyteFile),
    )

    @workflow
    def train_test_wf(
        train: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train",
        test: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
    ) -> List[float]:
        _, predictions, _ = xgboost_trainer(train=train, test=test)
        return predictions

    assert xgboost_trainer.python_interface.inputs == {"train": FlyteFile, "test": FlyteFile}
    train_test_wf()


def test_csv_data():
    """
    Note: For CSV training, the algorithm assumes that the CSV does not have a header record.
    To set the target variable, set the label_column parameter, which by default is 0.
    """
    xgboost_trainer = XGBoostTask(
        name="test2",
        hyperparameters=asdict(CSVHyperparameters()),
        inputs=kwtypes(train_data=CSVFile, test_data=CSVFile),
        label_column=0,
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
        return xgboost_trainer(train_data=train_data, test_data=test_data)

    assert xgboost_trainer.python_interface.inputs == {"train_data": CSVFile, "test_data": CSVFile}
    assert xgboost_trainer._label_column == 0
    wf()


def test_local_data():
    xgboost_trainer = XGBoostTask(
        name="test3",
        hyperparameters=asdict(CSVHyperparameters()),
        inputs=kwtypes(train_data=CSVFile, test_data=CSVFile),
        label_column=0,
    )

    @workflow
    def wf(train_data: CSVFile = "abalone_train.csv", test_data: CSVFile = "abalone_test.csv") -> List[float]:
        _, predictions, _ = xgboost_trainer(train_data=train_data, test_data=test_data)
        return predictions

    assert xgboost_trainer.python_interface.inputs == {"train_data": CSVFile, "test_data": CSVFile}
    wf()


def test_schema_data():
    """
    Note: To set the target variable, set the label_column parameter, which by default is 0.
    """
    xgboost_trainer = XGBoostTask(
        name="test4",
        hyperparameters=asdict(CSVHyperparameters()),
        inputs=kwtypes(train_data=FlyteSchema, test_data=FlyteSchema),
        label_column=0,
    )

    @task
    def csv_to_df(data: str) -> pd.DataFrame:
        return pd.read_csv(data)

    @workflow
    def wf(train_data: str = "abalone_train.csv", test_data: str = "abalone_test.csv") -> List[float]:
        _, predictions, _ = xgboost_trainer(train_data=csv_to_df(data=train_data), test_data=csv_to_df(data=test_data))
        return predictions

    assert xgboost_trainer.python_interface.inputs == {"train_data": FlyteSchema, "test_data": FlyteSchema}
    wf()


def test_pipeline():
    xgboost_trainer = XGBoostTask(
        name="test5",
        hyperparameters=asdict(Hyperparameters()),
        task_config=TrainParameters(num_boost_round=2),
        inputs=kwtypes(train=FlyteFile, test=FlyteFile, validation=FlyteFile),
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
        "wf_output", model=JoblibSerializedFile, accuracy=float, evaluation_result=Dict[str, Dict[str, List[float]]]
    )

    @workflow
    def full_pipeline(
        train: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.train",
        test: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
        validation: FlyteFile = "https://raw.githubusercontent.com/dmlc/xgboost/master/demo/data/agaricus.txt.test",
    ) -> wf_output:
        model, predictions, evaluation_result = xgboost_trainer(train=train, test=test, validation=validation)
        return model, estimate_accuracy(predictions=predictions, test=test), evaluation_result

    assert xgboost_trainer.python_interface.inputs == {"train": FlyteFile, "test": FlyteFile, "validation": FlyteFile}
    assert full_pipeline().accuracy >= 0.7
