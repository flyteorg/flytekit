import markdown
import pandas as pd
from flytekitplugins.deck.renderer import BoxRenderer, FrameProfilingRenderer, MarkdownRenderer

df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})


def test_frame_profiling_renderer():
    renderer = FrameProfilingRenderer()
    assert "Profile Report Generated With The `Pandas-Profiling`" in renderer.to_html(df).title()


def test_markdown_renderer():
    md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
    renderer = MarkdownRenderer()
    assert renderer.to_html(md_text) == markdown.markdown(md_text)


def test_box_renderer():
    renderer = BoxRenderer("Name")
    assert "Plotlyconfig = {Mathjaxconfig: 'Local'}" in renderer.to_html(df).title()
