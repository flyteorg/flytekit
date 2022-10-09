from typing import Tuple

import numpy as np
import vaex

from flytekit import task, workflow


@task
def generate_vaex_dataframe() -> vaex.DataFrame:
    x = np.arange(5)
    y = x**2
    z = x**3
    return vaex.from_arrays(x=x, y=y, z=z)


@task
def t1(df: vaex.DataFrame) -> vaex.DataFrame:
    assert df.dtype == vaex.DataFrame
    df["r"] = np.sqrt(df.x**2 + df.y**2 + df.z**2)
    return df


@task
def t2(df: vaex.DataFrame) -> vaex.DataFrame:
    return df[df.y > 5]


@task
def t3(df: vaex.DataFrame) -> Tuple[np.ndarray, ...]:
    return df.count(), df.mean(df.x), df.mean(df.x, selection=True)


@workflow
def wf():
    df = generate_vaex_dataframe()
    t1(df=df)
    t2(df=df)
    t3(df=df)


@workflow
def test_wf():
    wf()
