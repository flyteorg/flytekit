import enum
import os
import typing
from html import escape
from string import Template
from typing import Optional

from flytekit.core.context_manager import ExecutionParameters, ExecutionState, FlyteContext, FlyteContextManager
from flytekit.loggers import logger
from flytekit.tools.interactive import ipython_check

OUTPUT_DIR_JUPYTER_PREFIX = "jupyter"
DECK_FILE_NAME = "deck.html"


class DeckField(str, enum.Enum):
    """
    DeckField is used to specify the fields that will be rendered in the deck.
    """

    INPUT = "Input"
    OUTPUT = "Output"
    SOURCE_CODE = "Source Code"
    TIMELINE = "Timeline"
    DEPENDENCIES = "Dependencies"


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
    scatter plots or Markdown text. In addition, users can create new decks to render
    their data with custom renderers.

    .. code-block:: python

        iris_df = px.data.iris()

        @task()
        def t1() -> str:
            md_text = '#Hello Flyte##Hello Flyte###Hello Flyte'
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

    def __init__(self, name: str, html: Optional[str] = "", auto_add_to_deck: bool = True):
        self._name = name
        self._html = html
        if auto_add_to_deck:
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

    @staticmethod
    def publish():
        params = FlyteContextManager.current_context().user_space_params
        task_name = params.task_id.name

        if not params.enable_deck:
            logger.warning(
                f"Attempted to call publish() in task '{task_name}', but Flyte decks will not be generated because enable_deck is currently set to False."
            )
            return

        _output_deck(task_name=task_name, new_user_params=params)


class TimeLineDeck(Deck):
    """
    The TimeLineDeck class is designed to render the execution time of each part of a task.
    Unlike deck class, the conversion of data to HTML is delayed until the html property is accessed.
    This approach is taken because rendering a timeline graph with partial data would not provide meaningful insights.
    Instead, the complete data set is used to create a comprehensive visualization of the execution time of each part of the task.
    """

    def __init__(self, name: str, html: Optional[str] = "", auto_add_to_deck: bool = True):
        super().__init__(name, html, auto_add_to_deck)
        self.time_info = []

    def append_time_info(self, info: dict):
        assert isinstance(info, dict)
        self.time_info.append(info)

    @property
    def html(self) -> str:
        if len(self.time_info) == 0:
            return ""

        note = """
            <p><strong>Note:</strong></p>
            <ol>
                <li>if the time duration is too small(< 1ms), it may be difficult to see on the time line graph.</li>
                <li>For accurate execution time measurements, users should refer to wall time and process time.</li>
            </ol>
        """

        return generate_time_table(self.time_info) + note


def generate_time_table(data: dict) -> str:
    html = [
        '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; width: 100%;">',
        """
        <thead>
            <tr>
                <th>Name</th>
                <th>Wall Time(s)</th>
                <th>Process Time(s)</th>
            </tr>
        </thead>
        """,
        "<tbody>",
    ]

    # Add table rows
    for row in data:
        html.append("<tr>")
        html.append(f"<td>{row['Name']}</td>")
        html.append(f"<td>{row['WallTime']:.6f}</td>")
        html.append(f"<td>{row['ProcessTime']:.6f}</td>")
        html.append("</tr>")
    html.append("</tbody>")

    html.append("</table>")
    return "".join(html)


def _get_deck(
    new_user_params: ExecutionParameters,
    ignore_jupyter: bool = False,
) -> typing.Union[str, "IPython.core.display.HTML"]:  # type:ignore
    """
    Get flyte deck html string
    If ignore_jupyter is set to True, then it will return a str even in a jupyter environment.
    """
    deck_map = {deck.name: deck.html for deck in new_user_params.decks}
    nav_htmls = []
    body_htmls = []

    for key, value in deck_map.items():
        nav_htmls.append(f'<li onclick="handleLinkClick(this)">{escape(key)}</li>')
        # Can not escape here because this is HTML. Escaping it will present the HTML as text.
        # The renderer must ensure that the HTML is safe.
        body_htmls.append(f"<div>{value}</div>")

    raw_html = get_deck_template().substitute(NAV_HTML="".join(nav_htmls), BODY_HTML="".join(body_htmls))
    if not ignore_jupyter and ipython_check():
        try:
            from IPython.core.display import HTML
        except ImportError:
            ...
        return HTML(raw_html)
    return raw_html


def _output_deck(task_name: str, new_user_params: ExecutionParameters):
    ctx = FlyteContext.current_context()

    local_dir = ctx.file_access.get_random_local_directory()
    local_path = f"{local_dir}{os.sep}{DECK_FILE_NAME}"
    try:
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(_get_deck(new_user_params=new_user_params, ignore_jupyter=True))
        logger.info(f"{task_name} task creates flyte deck html to file://{local_path}")
        if ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            fs = ctx.file_access.get_filesystem_for_path(new_user_params.output_metadata_prefix)
            remote_path = f"{new_user_params.output_metadata_prefix}{ctx.file_access.sep(fs)}{DECK_FILE_NAME}"
            kwargs: typing.Dict[str, str] = {
                "ContentType": "text/html",  # For s3
                "content_type": "text/html",  # For gcs
            }
            ctx.file_access.put_data(local_path, remote_path, **kwargs)
    except Exception as e:
        logger.error(f"Failed to write flyte deck html with error {e}.")


def get_deck_template() -> Template:
    root = os.path.dirname(os.path.abspath(__file__))
    templates_dir = os.path.join(root, "html", "template.html")

    with open(templates_dir, "r") as f:
        template_content = f.read()
    return Template(template_content)
