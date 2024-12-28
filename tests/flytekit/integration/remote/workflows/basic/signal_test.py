from datetime import timedelta
from flytekit import task, workflow, wait_for_input, approve, conditional
import typing

@task
def reporting_wf(title_input: str, data: typing.List[float]) -> dict:
    return {"title": title_input, "data": data}

@workflow
def signal_test_wf(data: typing.List[float]) -> dict:
    title_input = wait_for_input(name="title-input", timeout=timedelta(hours=1), expected_type=str)

    # Define a "review-passes" approve node so that a human can review
    # the title before finalizing it.
    approve_node = approve(upstream_item=title_input, name="review-passes", timeout=timedelta(hours=1))
    title_input >> approve_node
    # This conditional returns the finalized report if the review passes,
    # otherwise it returns an invalid report output.
    return reporting_wf(title_input, data)
