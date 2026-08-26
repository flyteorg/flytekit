import datetime
import tempfile

import markdown
import pandas as pd
import pytest

from flytekitplugins.deck_weightwatcher.renderer import WeightWatcherRenderer
import torch.nn as nn 
import weightwatcher as ww
import torchvision.models as models
from flytekit import task, workflow


def create_toy_dataset():
    df = pd.DataFrame({'Name': ['Alice', 'Bob'], 'Age': [25, 30], 'Salary': [50000, 60000]})
    return df

def toy_model(df):
    input_size = len(df.columns) - 1
    output_size = 1
    model = nn.Linear(input_size, output_size)
    return model


def test_weightwatcher_renderer():
    renderer = WeightWatcherRenderer()
    df = create_toy_dataset()
    model = toy_model(df) 
    assert isinstance(renderer.to_html(model),str)