import unittest
from utils.infra import union_maps


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
