import json
from typing import List, Union

from flytekit import FlyteContextManager
from flytekit.visualize.renderer import Renderer


class Deck:
    def __init__(self, name: str, renderers: Union[List[Renderer], Renderer]):
        self.name = name
        self.renderers = renderers if isinstance(renderers, list) else [renderers]
        ctx = FlyteContextManager.current_context()
        ctx.user_space_params.decks.append(self)

    def append(self, r: Renderer) -> "Deck":
        self.renderers.append(r)
        return self

    def upload(self, path):
        div_map = {}
        for i in range(len(self.renderers)):
            file_name = self.name + "_" + str(i)
            html_path = path + "/" + file_name + ".html"
            with open(html_path, "w") as output:
                # self.path = ctx.file_access.get_random_remote_path()
                div_map[f"d{i}"] = html_path
                output.write(self.renderers[i].render())

        with open(path + "/metadata/" + self.name, "w") as output:
            output.write(json.dumps(div_map))
