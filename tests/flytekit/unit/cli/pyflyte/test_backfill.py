from datetime import datetime, timedelta

import click
import pytest

from flytekit.clis.sdk_in_container.backfill import resolve_backfill_window


def test_resolve_backfill_window():
    dt = datetime(2022, 12, 1, 8)
    window = timedelta(days=10)
    assert resolve_backfill_window(None, dt + window, window) == (dt, dt + window)
    assert resolve_backfill_window(dt, None, window) == (dt, dt + window)
    assert resolve_backfill_window(dt, dt + window) == (dt, dt + window)
    with pytest.raises(click.BadParameter):
        resolve_backfill_window()
