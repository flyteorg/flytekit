import pandas as pd

from flytekit.deck.renderer import TopFrameRenderable


def test_frame_profiling_renderer():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    renderer = TopFrameRenderable(df)
    assert renderer.to_html() == df.to_html()
