import pandas as pd
import pyarrow as pa

from flytekit.deck.renderer import ArrowRenderer, TopFrameRenderer


def test_renderer():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    pa_df = pa.Table.from_pandas(df)

    assert TopFrameRenderer().to_html(df) == df.to_html()
    assert ArrowRenderer().to_html(pa_df) == pa_df.to_string()
