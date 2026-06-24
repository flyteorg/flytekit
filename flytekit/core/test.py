import unittest
from unittest.mock import patch
import warnings
from flytekit.core.context_manager import SecretsManager  
# from flytekit.core.context_manager import _deprecate_positional_args

class TestSecretsManager(unittest.TestCase):
    def setUp(self):
        self.secrets_manager = SecretsManager()

    @patch("warnings.warn")
    @patch.object(SecretsManager, "get", return_value="mocked_secret_value")
    def test_get_with_positional_arguments(self, mock_get, mock_warn):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")  # Catch all warnings, including FutureWarning
            self.secrets_manager.get("my_group", "my_key")
            # Check if any warning of category FutureWarning is present
            self.assertTrue(
                any(isinstance(warning.message, FutureWarning) for warning in w),
                "Expected FutureWarning was not triggered"
            )
            mock_warn.assert_called_once_with(
                "Pass key=my_key as keyword args. From version 1.15.0, passing these as positional arguments will result in an error",
                FutureWarning,
            )

    @patch.object(SecretsManager, "get", return_value="mocked_secret_value")
    def test_get_with_keyword_arguments(self, mock_get):
        result = self.secrets_manager.get(group="my_group", key="my_key")
        self.assertEqual(result, "mocked_secret_value")

if __name__ == "__main__":
    unittest.main()