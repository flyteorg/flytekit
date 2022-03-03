import pandas as pd
import plotly.express as px

import flytekit
from flytekit import task, workflow, dynamic
from flytekit.deck import FrameProfilingRenderer, FrameRenderer, MarkdownRenderer, ScatterRenderer, default_deck


@task()
def t1(x: int) -> str:
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    m = MarkdownRenderer(md_text)
    s = ScatterRenderer(range(x), range(x))
    deck = flytekit.Deck("demo", s)
    deck.append(m)
    default_deck.append(m)
    return md_text


@task()
def t2() -> pd.DataFrame:
    df = px.data.iris()
    flytekit.Deck("my dataframe", [FrameRenderer(df)])
    return df


@dynamic
def t3():
    t1(x=10)
    t2()


@workflow
def wf():
    t3()


if __name__ == "__main__":
    wf()
