import unittest
from typing import Dict, NamedTuple
from unittest.mock import MagicMock, patch

from flytekitplugins.soda import SodaTask

from flytekit import task, workflow

# Define a NamedTuple to represent the expected output from the SodaTask
SodaTaskOutput = NamedTuple("SodaTaskOutput", [("scan_result", Dict[str, any])])

# Mock configurations for the SodaTask
MOCK_SCAN_DEFINITION = "mock_scan_definition.yaml"
MOCK_DATA_SOURCE = "mock_data_source"
MOCK_SCAN_NAME = "mock_scan_name"
MOCK_API_KEY = "mock_api_key"
MOCK_RESPONSE = {
    "scan_result": {"status": "success", "findings": []}
}  # Example response structure


# Define a Flyte task to initialize the SodaTask and execute it
@task
def setup_soda_task() -> SodaTaskOutput:
    # Initialize the SodaTask with mock parameters
    soda_task = SodaTask(
        scan_definition=MOCK_SCAN_DEFINITION,
        data_source=MOCK_DATA_SOURCE,
        scan_name=MOCK_SCAN_NAME,
        soda_cloud_api_key=MOCK_API_KEY,
    )

    # Execute the task and return the mock response
    return soda_task.execute()


# Define a Flyte workflow to test the setup task
@workflow
def test_soda_workflow() -> SodaTaskOutput:
    return setup_soda_task()


# Define the test class for the SodaTask plugin
class TestSodaTask(unittest.TestCase):
    @patch("requests.post")
    def test_soda_task_execution(self, mock_post):
        # Mock the response to simulate the Soda.io API call
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_RESPONSE
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        # Run the workflow
        result = test_soda_workflow()

        # Assertions to verify expected results
        self.assertEqual(result.scan_result, MOCK_RESPONSE["scan_result"])
        mock_post.assert_called_once_with(
            "https://api.soda.io/v1/scan",  # Replace with actual endpoint
            json={
                "scan_definition": MOCK_SCAN_DEFINITION,
                "data_source": MOCK_DATA_SOURCE,
                "scan_name": MOCK_SCAN_NAME,
                "api_key": MOCK_API_KEY,
            },
        )


if __name__ == "__main__":
    unittest.main()
