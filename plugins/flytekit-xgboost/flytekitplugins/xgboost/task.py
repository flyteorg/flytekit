from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import joblib
import xgboost
from dataclasses_json import dataclass_json

from flytekit import PythonInstanceTask
from flytekit.core.context_manager import FlyteContext
from flytekit.extend import Interface
from flytekit.types.file import JoblibSerializedFile
from flytekit.types.file.file import FlyteFile


@dataclass_json
@dataclass
class TrainParameters(object):
    """
    Parameters for training a model. These are given as arguments to the xgboost.train() method.

    Args:
        num_boost_round: Number of boosting iterations.
        early_stopping_rounds: Number of early stopping rounds.
        verbose_eval: If verbose_eval is True then the evaluation metric on the validation set is printed at each boosting stage.
    """

    num_boost_round: int = 10
    early_stopping_rounds: int = None
    verbose_eval: int = True


class XGBoostTask(PythonInstanceTask[TrainParameters]):
    """
    A task that runs an XGBoost model.

    Args:
        name: Name of the task.
        inputs: Inputs to the task.
        hyperparameters: Hyperparameters for the task.
        task_config: Configuration for the task.
        label_column: Index of the class label column in the CSV dataset.
    """

    _TASK_TYPE = "xgboost"

    def __init__(
        self,
        name: str,
        inputs: Dict[str, Type],
        hyperparameters: Optional[Dict[str, Union[str, int, float, bool]]] = None,
        task_config: Optional[TrainParameters] = None,
        label_column: Optional[int] = 0,
        **kwargs,
    ):
        self._hyperparameters = hyperparameters
        self._train_parameters = task_config
        self._label_column = label_column

        outputs = {
            "model": JoblibSerializedFile,
            "predictions": List[float],
            "evaluation_result": Dict[str, Dict[str, List[float]]],
        }

        super(XGBoostTask, self).__init__(
            name,
            task_type=self._TASK_TYPE,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    # Train method
    def train(self, dtrain, dvalid, **kwargs) -> Tuple[str, Dict[str, Dict[str, List[float]]]]:
        evals_result = {}
        # if validation data is provided, then populate evals and evals_result
        booster_model = (
            xgboost.train(
                params=self._hyperparameters,
                dtrain=dtrain,
                **asdict(self._train_parameters if self._train_parameters else TrainParameters()),
                evals=[(dvalid, "validation")],
                evals_result=evals_result,
            )
            if dvalid
            else xgboost.train(
                params=self._hyperparameters,
                dtrain=dtrain,
                **asdict(self._train_parameters if self._train_parameters else TrainParameters()),
            )
        )
        fname = "booster_model.joblib.dat"
        joblib.dump(booster_model, fname)
        return fname, evals_result

    # Test method
    def test(self, booster_model, dtest, **kwargs) -> List[float]:
        booster_model = joblib.load(booster_model)
        y_pred = booster_model.predict(dtest).tolist()
        return y_pred

    def execute(self, **kwargs) -> Any:
        train_key = ""
        test_key = ""
        validation_key = ""

        # fetch inputs
        for key in self.python_interface.inputs.keys():
            if "train" in key:
                train_key = key
            elif "test" in key:
                test_key = key
            elif "valid" in key:
                validation_key = key

        if not (train_key and test_key):
            raise ValueError("Must have train and test inputs")

        dataset_vars = {}

        for each_key in [train_key, test_key, validation_key]:
            if each_key:
                dataset = kwargs[each_key]
                if issubclass(self.python_interface.inputs[each_key], str):
                    if "csv" in dataset:
                        dataset_vars[each_key] = xgboost.DMatrix(
                            dataset + "?format=csv&label_column=" + str(self._label_column)
                        )
                    else:
                        dataset_vars[each_key] = xgboost.DMatrix(dataset)
                elif issubclass(self.python_interface.inputs[each_key], FlyteFile):
                    if FlyteContext.current_context().file_access.is_remote(dataset):
                        dataset.download()
                    if dataset.extension() == "csv":
                        dataset_vars[each_key] = xgboost.DMatrix(
                            dataset.path + "?format=csv&label_column=" + str(self._label_column)
                        )
                    else:
                        dataset_vars[each_key] = xgboost.DMatrix(dataset.path)
                else:
                    raise ValueError(f"Invalid type for {each_key} input")

        model, evals_result = self.train(
            dtrain=dataset_vars[train_key], dvalid=dataset_vars[validation_key] if validation_key else None
        )
        predictions = self.test(booster_model=model, dtest=dataset_vars[test_key])

        return model, predictions, evals_result
