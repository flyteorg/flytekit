import pandas as pd

from flytekit.deck import FrameRenderer


def test_frame_profiling_renderer():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    renderer = FrameRenderer(df)
    assert renderer.render() == df.to_html()
