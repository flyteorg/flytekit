import pytest
import unittest
from utils.constants import EI, EI_DEV, GRID1, PROD_LTX1
from kingkong.utils import union_maps, parse_kkid_from_release_name, get_kingkong_endpoint


class TestUnionMaps(unittest.TestCase):
    def test_union_maps_with_non_empty_dicts(self):
        map1 = {'a': 1, 'b': 2}
        map2 = {'b': 3, 'c': 4}
        expected = {'a': 1, 'b': 3, 'c': 4}
        result = union_maps(map1, map2)
        self.assertEqual(result, expected)

    def test_union_maps_with_empty_dict(self):
        map1 = {'a': 1, 'b': 2}
        map2 = {}
        expected = {'a': 1, 'b': 2}
        result = union_maps(map1, map2)
        self.assertEqual(result, expected)

    def test_union_maps_with_none_dict(self):
        map1 = {'a': 1}
        map2 = None
        map3 = {'b': 2}
        expected = {'a': 1, 'b': 2}
        result = union_maps(map1, map2, map3)
        self.assertEqual(result, expected)

    def test_union_maps_with_conflicting_keys(self):
        map1 = {'a': 1, 'b': 2}
        map2 = {'b': 5, 'c': 3}
        map3 = {'a': 6}
        expected = {'a': 6, 'b': 5, 'c': 3}
        result = union_maps(map1, map2, map3)
        self.assertEqual(result, expected)

    def test_union_maps_with_no_dicts(self):
        expected = {}
        result = union_maps()
        self.assertEqual(result, expected)


class TestParseKKID(unittest.TestCase):

    def test_valid_name(self):
        self.assertEqual(parse_kkid_from_release_name("gnn-abc123-def456"), "def456")

    def test_valid_name_with_numbers(self):
        self.assertEqual(parse_kkid_from_release_name("gnn-xyz789-123456"), "123456")

    def test_valid_name_with_special_chars(self):
        self.assertEqual(parse_kkid_from_release_name("gnn-hashcode-xyz_456!@#"), "xyz_456!@#")

    def test_invalid_name_format(self):
        with self.assertRaises(ValueError):
            parse_kkid_from_release_name("gnn-abc123")

    def test_empty_name(self):
        with self.assertRaises(ValueError):
            parse_kkid_from_release_name("")


def test_get_kingkong_endpoint_valid_clusters():
    assert get_kingkong_endpoint(EI) == "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution"
    assert get_kingkong_endpoint(EI_DEV) == "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution"
    assert get_kingkong_endpoint(GRID1) == "https://kingkong-server.grid1-k8s0.grid.linkedin.com/api/v1/execution"
    assert get_kingkong_endpoint(PROD_LTX1) == "https://2.kingkong-server.prod-ltx1.atd.disco.linkedin.com:30126/api/v1/execution"


def test_get_kingkong_endpoint_invalid_cluster():
    with pytest.raises(Exception) as excinfo:
        get_kingkong_endpoint("INVALID_CLUSTER")
    assert "is not valid or supported yet" in str(excinfo.value)


def test_get_kingkong_endpoint_case_insensitivity():
    assert get_kingkong_endpoint(EI_DEV) == "https://kingkong-server.ei-ltx1-k8s0.stg.linkedin.com/api/v1/execution"
    assert get_kingkong_endpoint(GRID1) == "https://kingkong-server.grid1-k8s0.grid.linkedin.com/api/v1/execution"
