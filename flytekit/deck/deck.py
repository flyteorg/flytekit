import os
from typing import List, Union

from flytekit.core.type_engine import TypeEngine

from flytekit.loggers import logger
from jinja2 import Environment, FileSystemLoader

from flytekit import FlyteContextManager, ExecutionParameters, FlyteContext
from flytekit.deck.renderer import Renderer


class Deck:
    def __init__(self, name: str, renderers: Union[List[Renderer], Renderer]):
        self.name = name
        self.renderers = renderers if isinstance(renderers, list) else [renderers]
        ctx = FlyteContextManager.current_context()
        ctx.user_space_params.decks.append(self)

    def append(self, r: Renderer) -> "Deck":
        self.renderers.append(r)
        return self


def _output_deck(new_user_params: ExecutionParameters, task_input: dict, task_output: dict):
    deck_map = {}
    INPUT = "input"
    OUTPUT = "output"
    ctx = FlyteContext.current_context()
    # output_dir = ctx.file_access.get_random_local_directory()
    output_dir = "/Users/kevin/git/flytekit/deck_outputs"

    deck_map[INPUT] = []
    for k, v in task_input.items():
        _deck_to_html_file(deck_map, INPUT, output_dir, k, TypeEngine.to_html(ctx, v, type(v)))

    deck_map[OUTPUT] = []
    for k, v in task_output.items():
        _deck_to_html_file(deck_map, OUTPUT, output_dir, k, TypeEngine.to_html(ctx, v, type(v)))

    for deck in new_user_params.decks:
        deck_map[deck.name] = []
        for i in range(len(deck.renderers)):
            file_name = deck.name + "_" + str(i)
            _deck_to_html_file(deck_map, deck.name, output_dir, file_name, deck.renderers[i].render())

    root = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(root, 'html')
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template('template.html')

    deck_path = os.path.join(output_dir, "deck.html")
    with open(deck_path, 'w') as f:
        f.write(template.render(metadata=deck_map))

    # TODO: upload deck to remote filesystems (s3, gcs)
    logger.info(f"Output flytekit deck html to {deck_path}")


def _deck_to_html_file(deck_map, deck_name, output_dir, file_name, html: str):
    file_name = file_name + ".html"
    div_path = os.path.join(output_dir, file_name)
    with open(div_path, "w") as output:
        deck_map[deck_name].append(file_name)
        output.write(html)


default_deck = Deck("default", [])
