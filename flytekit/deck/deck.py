import html
import os
import pathlib
import random
from typing import Dict, Optional
from uuid import UUID

from jinja2 import Environment, FileSystemLoader
from IPython.display import HTML, IFrame, display

from flytekit.core.context_manager import ExecutionParameters, FlyteContext, FlyteContextManager


class Deck:
    """
    Deck enable users to get customizable and default visibility into their tasks.

    Deck contains a list of renderers (FrameRenderer, MarkdownRenderer) that can
    generate a html file. For example, FrameRenderer can render a DataFrame as an HTML table,
    MarkdownRenderer can convert Markdown string to HTML

    Flyte context saves a list of deck objects, and we use renderers in those decks to render
    the data and create an HTML file when those tasks are executed

    Each task has a least three decks (input, output, default). Input/output decks are
    used to render tasks' input/output data, and the default deck is used to render line plots,
    scatter plots or markdown text. In addition, users can create new decks to render
    their data with custom renderers.

    .. warning::

        This feature is in beta.

        .. code-block:: python

            iris_df = px.data.iris()

            @task()
            def t1() -> str:
                md_text = "#Hello Flyte\n##Hello Flyte\n###Hello Flyte"
                m = MarkdownRenderer()
                s = BoxRenderer("sepal_length")
                deck = flytekit.Deck("demo", s.to_html(iris_df))
                deck.append(m.to_html(md_text))
                default_deck = flytekit.current_context().default_deck
                default_deck.append(m.to_html(md_text))
                return md_text


            # Use Annotated to override default renderer
            @task()
            def t2() -> Annotated[pd.DataFrame, TopFrameRenderer(10)]:
                return iris_df

    """

    def __init__(self, name: str, html: Optional[str] = ""):
        self._name = name
        # self.renderers = renderers if isinstance(renderers, list) else [renderers]
        self._html = html
        FlyteContextManager.current_context().user_space_params.decks.append(self)

    def append(self, html: str) -> "Deck":
        assert isinstance(html, str)
        self._html = self._html + "\n" + html
        return self

    @property
    def name(self) -> str:
        return self._name

    @property
    def html(self) -> str:
        return self._html


def ipython_check() -> bool:
    """
    Check if interface is launching from iPython (not colab)
    :return is_ipython (bool): True or False
    """
    is_ipython = False
    try:  # Check if running interactively using ipython.
        from IPython import get_ipython

        if get_ipython() is not None:
            is_ipython = True
    except (ImportError, NameError):
        pass
    return is_ipython


def get_output_dir() -> str:
    if not ipython_check():
        ctx = FlyteContext.current_context()
        return ctx.file_access.get_random_local_directory()
    key = UUID(int=random.getrandbits(128)).hex
    # output_dir must be in the same directory as jupyter notebook
    output_dir = os.path.join(pathlib.Path(), "jupyter", key)
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    return output_dir


def _output_deck(new_user_params: ExecutionParameters):
    deck_map: Dict[str, str] = {}
    decks = new_user_params.decks

    root = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(root, "html")
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template("template.html")

    output_dir = get_output_dir()

    for deck in decks:
        _deck_to_html_file(deck, deck_map, output_dir)
        deck_map[deck.name] = deck.html

    if ipython_check():
        display(HTML(template.render(metadata=deck_map)), metadata=dict(isolated=True))
    else:
        deck_path = os.path.join(output_dir, "deck.html")
        with open(deck_path, "w") as f:
            f.write(template.render(metadata=deck_map))


def _deck_to_html_file(deck: Deck, deck_map: Dict[str, str], output_dir: str):
    file_name = deck.name + ".html"
    path = os.path.join(output_dir, file_name)
    if ipython_check():
        deck_map[deck.name] = os.path.relpath(path)
    else:
        deck_map[deck.name] = file_name

    with open(path, "w") as output:
        output.write(deck.html)
