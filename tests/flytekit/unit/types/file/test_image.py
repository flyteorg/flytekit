import io

import requests

from flytekit import task, workflow
import PIL.Image


@task(disable_deck=False)
def t1() -> PIL.Image.Image:
    url = "https://miro.medium.com/v2/resize:fit:1400/1*0T9PjBnJB9H0Y4qrllkJtQ.png"
    response = requests.get(url)
    image_bytes = io.BytesIO(response.content)
    im = PIL.Image.open(image_bytes)
    return im


@task
def t2(im: PIL.Image.Image) -> PIL.Image.Image:
    return im


@workflow
def wf():
    t2(im=t1())


def test_image_transformer():
    wf()
