"""Async workflows prototype."""

import asyncio
import logging
from typing import NamedTuple

import pandas as pd
from sklearn.datasets import load_wine
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from flytekit import task, workflow
from flytekit.configuration import Config, PlatformConfig  # noqa: F401
from flytekit.experimental import eager
from flytekit.remote import FlyteRemote


class CustomException(Exception):
    ...


BestModel = NamedTuple("BestModel", model=LogisticRegression, metric=float)
FORMAT = "%(asctime)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


@task
def get_data() -> pd.DataFrame:
    """Get the wine dataset."""
    return load_wine(as_frame=True).frame


@task
def process_data(data: pd.DataFrame) -> pd.DataFrame:
    """Simplify the task from a 3-class to a binary classification problem."""
    return data.assign(target=lambda x: x["target"].where(x["target"] == 0, 1))


@task
def train_model(data: pd.DataFrame, hyperparameters: dict) -> LogisticRegression:
    """Train a model on the wine dataset."""
    features = data.drop("target", axis="columns")
    target = data["target"]
    return LogisticRegression(max_iter=3000, **hyperparameters).fit(features, target)


@task
def evaluate_model(data: pd.DataFrame, model: LogisticRegression) -> float:
    """Train a model on the wine dataset."""
    features = data.drop("target", axis="columns")
    target = data["target"]
    return float(accuracy_score(target, model.predict(features)))


remote = FlyteRemote(
    config=Config.for_sandbox(),
    # config=Config(
    #     platform=PlatformConfig(
    #         endpoint="development.uniondemo.run",
    #         auth_mode="client_credentials",
    #         client_id="flytepropeller",
    #     ),
    # ),
    default_project="flytesnacks",
    default_domain="development",
)


@eager(
    remote=remote,
    # client_secret_group="arn:aws:secretsmanager:us-east-2:590375264460:secret:",
    # client_secret_key="flyte-development-client-secret-S3ptvJ",
    # client_secret_group="eager-mode",
    # client_secret_key="client_secret",
)
async def main() -> BestModel:
    data = await get_data()
    processed_data = await process_data(data=data)

    # split the data
    try:
        train, test = train_test_split(processed_data, test_size=0.2)
    except Exception as exc:
        raise CustomException(str(exc)) from exc

    models = await asyncio.gather(
        *[
            train_model(data=train, hyperparameters={"C": x})
            # for x in [0.1, 0.01, 0.001, 0.0001, 0.00001]
            for x in [0.1]
        ]
    )
    results = await asyncio.gather(*[evaluate_model(data=test, model=model) for model in models])

    best_model, best_result = None, float("-inf")
    for model, result in zip(models, results):
        if result > best_result:
            best_model, best_result = model, result

    assert best_model is not None, "model cannot be None!"
    return best_model, best_result


@task
def print_model(best_model: LogisticRegression):
    print(best_model)


@workflow
def wf() -> BestModel:
    """Flytekit async workflow wrapper."""
    best_model = main()
    print_model(best_model=best_model.model)
    return best_model


if __name__ == "__main__":
    print("training model")
    model = asyncio.run(main())
    print(f"trained model: {model}")
