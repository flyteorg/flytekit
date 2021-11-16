import typing
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

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

    num_boost_round: int = 4
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

    NOTE: There are a lot more hyperparameters available. We are using only a couple of them for demo purposes.
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


@dataclass_json
@dataclass
class XGBoostParameters:
    """
    XGBoost Parameter = Model Parameters + Hyperparameters

    Args:
        model_parameters: Model Parameters for training a model.
        hyper_parameters: Hyperparameters for training a model.
        label_column: Index of the column containing the labels.
    """

    model_parameters: Optional[ModelParameters] = None
    hyper_parameters: Optional[HyperParameters] = None
    label_column: int = 0


def load_flytefile(dataset: FlyteFile, label_column: int) -> xgboost.DMatrix:
    """
    Function to load a FlyteFile as a xgboost.DMatrix
    """
    filepath = dataset.download()

    # Load CSV
    if dataset.extension() == "csv":
        return xgboost.DMatrix(filepath + "?format=csv&label_column=" + str(label_column))
    # Load libSVM
    return xgboost.DMatrix(filepath)


def load_flyteschema(dataset: FlyteSchema, label_column: int) -> xgboost.DMatrix:
    """
    Function to load a FlyteSchema as an xgboost.DMatrix
    """
    # Load as a pandas DataFrame
    df = dataset.open().all()
    target = df[df.columns[label_column]]
    df = df.drop(df.columns[label_column], axis=1)
    return xgboost.DMatrix(df.values, target.values)


class XGBoostTrainerTask(PythonInstanceTask[XGBoostParameters]):
    _TASK_TYPE = "xgboost"
    _PARAMS_ARG = "params"
    _TRAIN_ARG = "train"
    _TEST_ARG = "test"
    _VALIDATION_ARG = "validation"

    _OUTPUT_MODEL = "model"
    _OUTPUT_PREDICTIONS = "predictions"
    _OUTPUT_EVAL_RESULT = "evaluation_result"

    def __init__(
        self,
        name: str,
        dataset_type: typing.Union[typing.Type[FlyteFile], typing.Type[FlyteSchema]] = typing.Type[FlyteFile],
        validate: bool = False,
        config: Optional[XGBoostParameters] = None,
        **kwargs,
    ):
        """
        A task that runs an XGBoost model.
        This task takes 3 implicit inputs - train, test, and validation. The type for these inputs depends on
        dataset_type.
        This task produces 3 outputs - the model, predictions for the test dataset, and evaluation result.

        Args:
            name: Name of the task.
            dataset_type: Type of the dataset, supported types are FlyteFile[csv, libsvm], FlyteSchema
            validate: Indicate if a validation dataset will be provided
            config: Configuration for the task.
        Returns:
            model: The trained model.
            predictions: The predictions for the test dataset.
            evaluation_result: The evaluation result for the validation dataset.
        """
        self._config = config
        self._dataset_type = dataset_type

        # NOTE: how we are defining implicit inputs for this model
        inputs = {
            self._TRAIN_ARG: dataset_type,
            self._TEST_ARG: dataset_type,
            self._PARAMS_ARG: XGBoostParameters,
        }

        self._validate = validate
        if validate:
            inputs[self._VALIDATION_ARG] = dataset_type

        # These are the implicit outputs
        outputs = {
            self._OUTPUT_MODEL: JoblibSerializedFile,
            self._OUTPUT_PREDICTIONS: List[float],
            self._OUTPUT_EVAL_RESULT: Dict[str, Dict[str, List[float]]],
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
        self,
        dtrain: xgboost.DMatrix,
        dvalid: xgboost.DMatrix,
        params: XGBoostParameters,
    ) -> Tuple[str, Dict[str, Dict[str, List[float]]]]:
        """
        This implements a default XGboost training method
        """
        evals_result = {}
        validation = []

        if dvalid:
            validation = [(dvalid, "validation")]

        booster_model = xgboost.train(
            params=asdict(params.hyper_parameters if params.hyper_parameters else HyperParameters()),
            dtrain=dtrain,
            **asdict(params.model_parameters if params.model_parameters else ModelParameters()),
            evals=validation,
            evals_result=evals_result,
        )
        fname = Path(flytekit.current_context().working_directory) / "model.joblib.dat"
        joblib.dump(booster_model, fname)
        return str(fname), evals_result

    # Test method
    def test(self, booster_model: str, dtest: xgboost.DMatrix) -> List[float]:
        """
        Using joblib, we load the model and generate predictions.
        """
        booster_model = joblib.load(booster_model)
        y_pred = booster_model.predict(dtest).tolist()
        return y_pred

    def execute(self, **kwargs) -> Any:
        """
        This is the entrypoint for your plugin. Check out the base API. There are numerous other methods available.
        For example: pre_execute and post_execute.
        """
        params: XGBoostParameters = kwargs[self._PARAMS_ARG]
        test = kwargs[self._TEST_ARG]
        train = kwargs[self._TRAIN_ARG]
        if self._validate:
            valid = kwargs[self._VALIDATION_ARG]

        if not params.model_parameters:
            params.model_parameters = self._config.model_parameters
        if not params.hyper_parameters:
            params.hyper_parameters = self._config.hyper_parameters

        print(f"Finalized parameters = {params}")

        #######################
        # Now, let us write the plugin body.

        # Step 1: use the helper methods - load_flytefile and load_flyteschema to get dtrain, dtest and dvalid
        # dtrain -> is the training dataset in the format xgboost.DMatrix. We can load a FlyteFile using load Flytefile
        # and we can load a FlyteSchema using load_flyteschema.

        # Step 2: Use the helper method train to train the xgboost model. check the training code too.

        # Step 3: Use the test method to test the model and compute the predictions.

        #######################

        dvalid = None
        if issubclass(self._dataset_type, FlyteFile):
            dtrain = load_flytefile(train, params.label_column)
            # We could delay the loading to reduce memory pressure
            dtest = load_flytefile(test, params.label_column)
            if self._validate:
                dvalid = load_flytefile(valid, params.label_column)
        elif issubclass(self._dataset_type, FlyteSchema):
            dtrain = load_flyteschema(train, params.label_column)
            # We could delay the loading to reduce memory pressure
            dtest = load_flyteschema(test, params.label_column)
            if self._validate:
                dvalid = load_flyteschema(valid, params.label_column)
        else:
            raise ValueError("Invalid type for input")

        model, evals_result = self.train(dtrain=dtrain, dvalid=dvalid, params=params)
        predictions = self.test(booster_model=model, dtest=dtest)

        return JoblibSerializedFile(model), predictions, evals_result
