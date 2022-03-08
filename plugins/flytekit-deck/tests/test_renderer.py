import markdown
import pandas as pd
from flytekitplugins.deck.renderer import FrameProfilingRenderer, MarkdownRenderer, ScatterRenderer


def test_frame_profiling_renderer():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    renderer = FrameProfilingRenderer(df)
    assert "Profile Report Generated With The `Pandas-Profiling`" in renderer.render().title()


def test_markdown_renderer():
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    renderer = MarkdownRenderer(md_text)
    assert renderer.render() == markdown.markdown(md_text)


def test_scatter_renderer():
    x = y = range(5)
    renderer = ScatterRenderer(x, y)
    assert "Plotlyconfig = {Mathjaxconfig: 'Local'}" in renderer.render().title()
