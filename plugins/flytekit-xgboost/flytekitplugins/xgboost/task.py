from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union, get_type_hints

import joblib
import xgboost
from dataclasses_json import dataclass_json

import flytekit
from flytekit import PythonInstanceTask
from flytekit.extend import Interface
from flytekit.types.file import JoblibSerializedFile
from flytekit.types.file.file import FlyteFile
from flytekit.types.schema.types import FlyteSchema


@dataclass_json
@dataclass
class ModelParameters(object):
    """
    Model Parameters for training a model. These are given as arguments to the xgboost.train() method.

    Args:
        num_boost_round: Number of boosting iterations.
        early_stopping_rounds: Number of early stopping rounds.
        verbose_eval: If verbose_eval is True then the evaluation metric on the validation set is printed at each boosting stage.
    """

    num_boost_round: int = 10
    early_stopping_rounds: Optional[int] = None
    verbose_eval: Optional[Union[bool, int]] = True


@dataclass_json
@dataclass
class HyperParameters(object):
    """
    Hyperparameters for training a model.

    Args:
        objective: Specifies the learning task and the corresponding learning objective.
        eta: Step size shrinkage used in update to prevents overfitting.
        gamma: Minimum loss reduction required to make a further partition on a leaf node of the tree.
        max_depth: Maximum depth of a tree.
        subsample: Subsample ratio of the training instance.
        verbosity: The verbosity level.
        booster: Specifies the booster type to use.
        tree_method: Specifies the tree construction algorithm.
        min_child_weight: Minimum sum of instance weight(hessian) needed in a child.
    """

    verbosity: int = 2
    objective: str = "reg:squarederror"
    eta: float = 0.3
    gamma: float = 0.0
    max_depth: int = 6
    subsample: float = 1.0
    booster: str = "gbtree"
    tree_method: str = "auto"
    min_child_weight: int = 1


class Updateable(object):
    def update(self, new):
        for key, value in new.items():
            if hasattr(self, key):
                setattr(self, key, value)


@dataclass_json
@dataclass
class XGBoostParameters(Updateable):
    """
    XGBoost Parameter = Model Parameters + Hyperparameters

    Args:
        model_parameters: Model Parameters for training a model.
        hyper_parameters: Hyperparameters for training a model.
        label_column: Index of the column containing the labels.
    """

    model_parameters: ModelParameters = ModelParameters()
    hyper_parameters: HyperParameters = HyperParameters()
    label_column: int = 0


class XGBoostTrainerTask(PythonInstanceTask[XGBoostParameters]):

    _TASK_TYPE = "xgboost"

    def __init__(
        self,
        name: str,
        inputs: Dict[str, Type],
        config: Optional[XGBoostParameters] = None,
        **kwargs,
    ):
        """
        A task that runs an XGBoost model.

        Args:
            name: Name of the task.
            inputs: Inputs to the task.
            config: Configuration for the task.
        Returns:
            model: The trained model.
            predictions: The predictions for the test dataset.
            evaluation_result: The evaluation result for the validation dataset.
        """
        self._config = config

        outputs = {
            "model": JoblibSerializedFile,
            "predictions": List[float],
            "evaluation_result": Dict[str, Dict[str, List[float]]],
        }

        super(XGBoostTrainerTask, self).__init__(
            name,
            task_type=self._TASK_TYPE,
            task_config=config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    # Train method
    def train(
        self, dtrain: xgboost.DMatrix, dvalid: xgboost.DMatrix, **kwargs
    ) -> Tuple[str, Dict[str, Dict[str, List[float]]]]:
        evals_result = {}
        # if validation data is provided, then populate evals and evals_result
        if dvalid:
            booster_model = xgboost.train(
                params=asdict(self._config.hyper_parameters),
                dtrain=dtrain,
                **asdict(self._config.model_parameters if self._config.model_parameters else ModelParameters()),
                evals=[(dvalid, "validation")],
                evals_result=evals_result,
            )
        else:
            booster_model = xgboost.train(
                params=asdict(self._config.hyper_parameters),
                dtrain=dtrain,
                **asdict(self._config.model_parameters if self._config.model_parameters else ModelParameters()),
            )
        fname = Path(flytekit.current_context().working_directory) / "model.joblib.dat"
        joblib.dump(booster_model, fname)
        return str(fname), evals_result

    # Test method
    def test(self, booster_model: str, dtest: xgboost.DMatrix, **kwargs) -> List[float]:
        booster_model = joblib.load(booster_model)
        y_pred = booster_model.predict(dtest).tolist()
        return y_pred

    def execute(self, **kwargs) -> Any:
        if not all(x in kwargs for x in ["train", "test"]):
            raise ValueError("Must have train and test inputs; 'train' and 'test' should be the actual parameter keys")

        # replace model_parameters, hyper_parameters, label_column with the user-given values
        for key, type in get_type_hints(XGBoostParameters).items():
            if key in kwargs:
                if issubclass(self.python_interface.inputs[key], type):
                    self._config.update({key: kwargs[key]})
                else:
                    raise TypeError(f"{key} has to be of the type {type}")

        dataset_vars = {}

        for each_key in ["train", "test", "validation"]:
            if each_key in kwargs:
                dataset = kwargs[each_key]
                # FlyteFile
                if issubclass(self.python_interface.inputs[each_key], FlyteFile):
                    filepath = dataset.download()
                    if dataset.extension() == "csv":
                        dataset_vars[each_key] = xgboost.DMatrix(
                            filepath + "?format=csv&label_column=" + str(self._config.label_column)
                        )
                    else:
                        dataset_vars[each_key] = xgboost.DMatrix(filepath)
                # FlyteSchema
                elif issubclass(self.python_interface.inputs[each_key], FlyteSchema):
                    df = dataset.open().all()
                    target = df[df.columns[self._config.label_column]]
                    df = df.drop(df.columns[self._config.label_column], axis=1)
                    dataset_vars[each_key] = xgboost.DMatrix(df.values, target.values)
                else:
                    raise ValueError(f"Invalid type for {each_key} input")

        model, evals_result = self.train(
            dtrain=dataset_vars["train"],
            dvalid=dataset_vars["validation"] if "validation" in kwargs else None,
        )
        predictions = self.test(booster_model=model, dtest=dataset_vars["test"])

        return JoblibSerializedFile(model), predictions, evals_result
