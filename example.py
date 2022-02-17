import pandas as pd
import plotly.express as px

import flytekit
from flytekit import task, workflow
from flytekit.visualize import FrameProfilingRenderer, FrameRenderer, MarkdownRenderer, ScatterRenderer


@task
def t1() -> str:
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    m = MarkdownRenderer(md_text)
    s = ScatterRenderer(range(10), range(10))
    flytekit.visualize.Deck("Tab1", s).append(m)
    flytekit.visualize.Deck("Tab2", m)
    return md_text


@task
def t2() -> pd.DataFrame:
    df = px.data.iris()
    flytekit.visualize.Deck("Tab3", [FrameRenderer(df), FrameProfilingRenderer(df)])
    return df


@workflow
def wf():
    t1()
    t2()


if __name__ == "__main__":
    wf()
