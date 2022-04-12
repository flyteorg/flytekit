import html
import os
from typing import Dict, Optional

from IPython.display import HTML, IFrame, display
from jinja2 import Environment, FileSystemLoader

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


def _output_deck(new_user_params: ExecutionParameters):
    deck_map: Dict[str, str] = {}
    decks = new_user_params.decks
    for deck in decks:
        deck_map[deck.name] = deck.html

    root = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(root, "html")
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template("template.html")

    if ipython_check():
        iframe = f'<iframe srcdoc="{html.escape(template.render(metadata=deck_map))}" width=100% height=400></iframe> '
        display(HTML(iframe))
    else:
        ctx = FlyteContext.current_context()
        output_dir = ctx.file_access.get_random_remote_directory()
        if not ctx.file_access.is_remote(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        deck_path = os.path.join(output_dir, "deck.html")
        with open(deck_path, "w") as f:
            f.write(template.render(metadata=deck_map))
