import markdown
import pandas as pd
from flytekitplugins.deck.renderer import BoxRenderer, FrameProfilingRenderer, MarkdownRenderer

df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})


def test_frame_profiling_renderer():
    renderer = FrameProfilingRenderer(df)
    assert "Profile Report Generated With The `Pandas-Profiling`" in renderer.to_html().title()


def test_markdown_renderer():
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    renderer = MarkdownRenderer(md_text)
    assert renderer.to_html() == markdown.markdown(md_text)


def test_box_renderer():
    renderer = BoxRenderer(df, "Name")
    assert "Plotlyconfig = {Mathjaxconfig: 'Local'}" in renderer.to_html().title()
