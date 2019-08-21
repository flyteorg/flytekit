from __future__ import absolute_import

import pytest

from flytekit.configuration.internal import look_up_version_from_image_tag


def test_parsing():
    str = 'somedocker.com/myimage:someversion123'
    version = look_up_version_from_image_tag(str)
    assert version == 'someversion123'

    str = 'ffjdskl/jfkljkdfls'
    with pytest.raises(Exception):
        look_up_version_from_image_tag(str)
