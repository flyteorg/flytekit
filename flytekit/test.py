from flytekit import dynamic,task, workflow

# from flytekitplugins.papermill import NotebookTask

"""Testing Module and Tasks/Workflows"""


@task(pod_template_name="flyte-template-default")
def test_task() -> str:
    return "Hello!"


@dynamic
def dynamic_wf() -> str:
    return test_task()


@workflow
def main_wf() -> str:  # List[int]:
    return dynamic_wf()

if __name__ == "__main__":
    scopes = ["string1", "string2", "string3", "string4"]
    output = " ".join(s.strip("' ") for s in scopes).strip("[]'")
    print(f"{output}")