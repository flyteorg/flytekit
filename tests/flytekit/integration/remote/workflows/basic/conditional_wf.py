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
def conditional_wf(data: typing.List[float]) -> dict:
    title_input = wait_for_input("title-input", timeout=timedelta(hours=1), expected_type=str)

    # Define a "review-passes" approve node so that a human can review
    # the title before finalizing it.
    reporting_wf_node = reporting_wf(
        title_input=approve(title_input, "review-passes", timeout=timedelta(hours=2)), 
        data=data
    )
    
    # This conditional returns the finalized report if the review passes,
    # otherwise it returns an invalid report output.
    return (
        conditional("final-report-condition")
        .if_(reporting_wf_node is not None)
        .then(reporting_wf(data=data))
        .else_()
        .then(invalid_report())
    )