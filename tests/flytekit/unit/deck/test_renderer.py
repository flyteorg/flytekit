import pandas as pd

from flytekit.deck.renderer import TopFrameRenderer


def test_frame_profiling_renderer():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    renderer = TopFrameRenderer()
    assert renderer.to_html(df) == df.to_html()
