try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

import pandas as pd
import plotly.express as px
from flytekitplugins.deck.renderer import BoxRenderer, MarkdownRenderer

import flytekit
from flytekit import dynamic, task, workflow
from flytekit.deck import default_deck
from flytekit.deck.renderer import TopFrameRenderer

iris_df = px.data.iris()


@task()
def t1() -> str:
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    m = MarkdownRenderer()
    s = BoxRenderer("sepal_length")
    deck = flytekit.Deck("demo", s.to_html(iris_df))
    deck.append(m.to_html(md_text))
    default_deck.append(m.to_html(md_text))
    return md_text


# Use Annotated to override default renderer
@task()
def t2() -> Annotated[pd.DataFrame, TopFrameRenderer(10)]:
    return iris_df


@dynamic
def t3():
    t1()
    t2()


@workflow
def wf():
    t3()


if __name__ == "__main__":
    wf()
