import json
from enum import Enum
from functools import partial
from typing import Dict, List, NamedTuple, Optional, Union

from flytekit import PythonInstanceTask, Secret, current_context, lazy_module
from flytekit.extend import Interface
from flytekit.types.structured.structured_dataset import StructuredDataset

duckdb = lazy_module("duckdb")
pd = lazy_module("pandas")
pa = lazy_module("pyarrow")


def connect_local(hosted_secret: Optional[Secret]):
    """Connect to local DuckDB."""
    return duckdb.connect(":memory:")


def connect_motherduck(hosted_secret: Secret):
    """Connect to MotherDuck."""
    motherduck_token = current_context().secrets.get(
        group=hosted_secret.group,
        key=hosted_secret.key,
        group_version=hosted_secret.group_version,
    )
    return duckdb.connect("md:", config={"motherduck_token": motherduck_token})


class DuckDBProvider(Enum):
    LOCAL = partial(connect_local)
    MOTHERDUCK = partial(connect_motherduck)


class QueryOutput(NamedTuple):
    counter: int = -1
    output: Optional[str] = None


class DuckDBQuery(PythonInstanceTask):
    _TASK_TYPE = "duckdb"

    def __init__(
        self,
        name: str,
        query: Union[str, List[str]],
        inputs: Optional[Dict[str, Union[StructuredDataset, list]]] = None,
        provider: DuckDBProvider = DuckDBProvider.LOCAL,
        **kwargs,
    ):
        """
        This method initializes the DuckDBQuery.

        Args:
            name: Name of the task
            query: DuckDB query to execute
            inputs: The query parameters to be used while executing the query
            provider: DuckDB provider (e.g., LOCAL, MOTHERDUCK, ANOTHERPRODUCT)
        """
        self._query = query
        self._provider = provider
        secret_requests: Optional[list[Secret]] = kwargs.get("secret_requests", None)
        self._hosted_secret = secret_requests[0] if secret_requests else None

        outputs = {"result": StructuredDataset}

        super(DuckDBQuery, self).__init__(
            name=name,
            task_type=self._TASK_TYPE,
            task_config=None,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def _connect_to_duckdb(self):
        """
        Handles the connection to DuckDB based on the provider.

        Returns:
            A DuckDB connection object.
        """

        if self._provider not in DuckDBProvider:
            raise ValueError(f"Unknown DuckDB provider: {self._provider}")

        if self._provider != DuckDBProvider.LOCAL and self._hosted_secret is None:
            raise ValueError(f"A secret is required for the {self._provider} provider.")

        return self._provider.value(self._hosted_secret)

    def _execute_query(
        self, con: duckdb.DuckDBPyConnection, params: list, query: str, counter: int, multiple_params: bool
    ):
        """
        This method runs the DuckDBQuery.

        Args:
            params: Query parameters to use while executing the query
            query: DuckDB query to execute
            counter: Use counter to map user-given arguments to the query parameters
            multiple_params: Set flag to indicate the presence of params for multiple queries
        """
        if any(x in query for x in ("$", "?")):
            if multiple_params:
                counter += 1
                if not counter < len(params):
                    raise ValueError("Parameter doesn't exist.")
                if "insert" in query.lower():
                    # run executemany disregarding the number of entries to store for an insert query
                    yield QueryOutput(output=con.executemany(query, params[counter]), counter=counter)
                else:
                    yield QueryOutput(output=con.execute(query, params[counter]), counter=counter)
            else:
                if params:
                    yield QueryOutput(output=con.execute(query, params), counter=counter)
                else:
                    raise ValueError("Parameter not specified.")
        else:
            yield QueryOutput(output=con.execute(query), counter=counter)

    def execute(self, **kwargs) -> StructuredDataset:
        # TODO: Enable iterative download after adding the functionality to structured dataset code.
        con = self._connect_to_duckdb()

        params = None
        for key in self.python_interface.inputs.keys():
            val = kwargs.get(key)
            if isinstance(val, StructuredDataset):
                # register structured dataset
                con.register(key, val.open(pa.Table).all())
            elif isinstance(val, (pd.DataFrame, pa.Table)):
                # register pandas dataframe/arrow table
                con.register(key, val)
            elif isinstance(val, list):
                # copy val into params
                params = val
            elif isinstance(val, str):
                # load into a list
                params = json.loads(val)
            else:
                raise ValueError(f"Expected inputs of type StructuredDataset, str or list, received {type(val)}")

        final_query = self._query
        query_output = QueryOutput()
        # set flag to indicate the presence of params for multiple queries
        multiple_params = isinstance(params[0], list) if params else False

        if isinstance(self._query, list) and len(self._query) > 1:
            # loop until the penultimate query
            for query in self._query[:-1]:
                query_output = next(
                    self._execute_query(
                        con=con,
                        params=params,
                        query=query,
                        counter=query_output.counter,
                        multiple_params=multiple_params,
                    )
                )
            final_query = self._query[-1]

        # fetch query output from the last query
        # expecting a SELECT query
        dataframe = next(
            self._execute_query(
                con=con, params=params, query=final_query, counter=query_output.counter, multiple_params=multiple_params
            )
        ).output.arrow()

        return StructuredDataset(dataframe=dataframe)
