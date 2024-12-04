from datetime import timedelta
from flytekit import task, workflow, wait_for_input, approve, conditional
import typing

@task
def reporting_wf(title_input: str, data: typing.List[float]) -> dict:
    return {"title": title_input, "data": data}

# Working with conditionals
# To illustrate this, let's extend the report-publishing use case so that we
# This example produces an "invalid report" output in case we don't approve the final report:
@task
def invalid_report() -> dict:
    return {"invalid_report": True}


@workflow
def signal_test_wf(data: typing.List[float]) -> dict:
    title_input = wait_for_input(name="title-input", timeout=timedelta(hours=1), expected_type=str)

    # Define a "review-passes" approve node so that a human can review
    # the title before finalizing it.
    approve_node = approve(upstream_item=title_input, name="review-passes", timeout=timedelta(hours=1))
    title_input >> approve_node
    # This conditional returns the finalized report if the review passes,
    # otherwise it returns an invalid report output.
    return (
        conditional("final-report-condition")
        .if_((approve_node == "True"))
        .then(reporting_wf(title_input, data))
        .else_()
        .then(invalid_report())
    )
