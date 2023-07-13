import pandas as pd
from sklearn.datasets import load_wine
from sklearn.linear_model import LogisticRegression

from flytekit import task, workflow


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


@workflow
def training_workflow(hyperparameters: dict) -> LogisticRegression:
    """Put all of the steps together into a single workflow."""
    data = get_data()
    processed_data = process_data(data=data)
    return train_model(
        data=processed_data,
        hyperparameters=hyperparameters,
    )


if __name__ == "__main__":
    from flytekit.configuration import Config
    from flytekit.remote import FlyteRemote

    remote = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
    )

    wf = remote.register_script(
        training_workflow,
        source_path=".",
        module_name="flyte_workflow",
    )

    print(f"registered workflow {wf.id}")
    execution = remote.execute(wf, inputs={"hyperparameters": {"C": 0.1}})
    print(remote.generate_console_url(execution))
