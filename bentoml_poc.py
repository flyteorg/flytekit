import subprocess
import os
from pathlib import Path

import bentoml
import numpy as np
import yaml
from bentoml.io import NumpyNdarray
from sklearn import svm
from sklearn import datasets

from flytekit import task, workflow
from flytekit.types.file import FlyteFile
# from flytekitplugins.bentoml import BentoMLBuildTask, BentoMLDeployTask


@task
def train_model() -> svm.SVC:
    # Load training data set
    iris = datasets.load_iris()
    X, y = iris.data, iris.target

    # Train the model
    clf = svm.SVC(gamma='scale')
    clf.fit(X, y)

    # Save model to the BentoML local model store
    return clf


class bentoml_service:

    def __init__(self, fn):
        self.fn = fn
        if os.environ.get("BENTOML_MODEL_TAG"):
            # The model needs to be load (bentoml.*.get(model_tag)) when the service is imported
            # by bentoml in the serving step. Therefore, the service needs to be defined when
            # the bentoml_service instance is initialized.
            self._svc = self.fn(self.model_tag)
        else:
            self._svc = None

    @property
    def model_tag(self):
        return os.getenv("BENTOML_MODEL_TAG")

    @property
    def svc(self):
        if self._svc is None:
            raise RuntimeError("Service instance not defined.")
        return self._svc


@bentoml_service
def service(model_tag: str):
    model = bentoml.sklearn.get(model_tag)
    iris_clf_runner = model.to_runner()
    svc = bentoml.Service("iris_classifier", runners=[iris_clf_runner], models=[model])
    
    @svc.api(input=NumpyNdarray(), output=NumpyNdarray())
    def classify(input_series: np.ndarray) -> np.ndarray:
        result = iris_clf_runner.predict.run(input_series)
        return result

    return svc


# manually
@task
def bentoml_build_task(model: svm.SVC) -> FlyteFile:
    saved_model = bentoml.sklearn.save_model("iris_clf", model)
    print(f"Model saved: {saved_model}")
    os.environ["BENTOML_MODEL_TAG"] = str(saved_model.tag)
    print("Building bento.")
    bento: bentoml.Bento = bentoml.bentos.build(
        service="bentoml_poc:service.svc",
        labels={"owner": "my-team", "stage": "dev"},
        include=["*.py"],
        exclude=["flytekit/", "tests/"],
        python={
            "packages": [
                "flytekit",
                "numpy",
                "pyyaml",
                "scikit-learn",
                "importlib_metadata",
            ],
        },
        docker={
            "env": {
                "BENTOML_MODEL_TAG": str(saved_model.tag),
            }
        }
    )
    print(f"Built bento {bento} exported to {bento.path}")
    export_path = f"./{bento.tag}.bento"
    bentoml.export_bento(bento.tag, path=export_path)
    bentoml.delete(bento.tag)
    return FlyteFile(path=export_path)


@task
def bentoml_deploy_task(bento_file: FlyteFile):
    from bentoctl.deployment_config import DeploymentConfig

    bento_file.download()
    bento = bentoml.import_bento(bento_file.path)
    subprocess.run(["bentoctl", "operator", "install", "aws-lambda"])

    deployment_config = {
        "api_version": "v1",
        "name": "iris_clf",
        "operator": {
            "name": "aws-lambda",
        },
        "template": "terraform",
        "spec": {
            "region": "us-east-2",
            "timeout": 30,
            "memory_size": 512,
        }
    }

    with Path("./deployment_config.yaml").open("w") as f:
        yaml.dump(deployment_config, f)

    deployment_config = DeploymentConfig(deployment_config)

    main_terraform_file = Path("./main.tf")
    if main_terraform_file.exists():
        os.remove(main_terraform_file)

    generated_files = deployment_config.generate(".")
    for f in generated_files:
        print(f"Generated file {f}")

    assert "bentoctl.tfvars" in generated_files

    subprocess.run(["bentoctl", "build", "-b", str(bento.tag), "-f", "./deployment_config.yaml"])
    subprocess.run(["terraform", "init"])
    subprocess.run(["terraform", "apply", "-var-file=bentoctl.tfvars", "--auto-approve"])


@workflow
def wf():
    model = train_model()
    bento = bentoml_build_task(model=model)
    bentoml_deploy_task(bento_file=bento)

# Open questions:
# - we need to make sure to abide by bentoml's conventions, e.g. the directory location of the bento
# - how the service is injected into the Flyte pod when the bento is being built
# - configuration of ECR credentials
# - when deploy task builds in a Flyte container, it needs priviledged access to run docker in docker


if __name__ == "__main__":
    wf()



# bentoml_build_task = BentoMLBuildTask(
#     name=...,
#     framework="sklearn",
#     service_fn=service,
#     **{...},
# )


# bentoml_deploy_task = BentoMLDeployTask(
#     config="""
#     api_version: v1
#     name: iris_clf
#     operator:
#         name: aws-lambda
#     template: terraform
#     spec:
#         region: us-east-2
#         timeout: 30
#         memory_size: 512
#     """,
#     operator="aws-lambda",
# )
