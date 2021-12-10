from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.core.type_engine import TypeEngine
from flytekit.core.context_manager import FlyteContextManager


@dataclass_json
@dataclass
class MyProxyConfiguration:
    # File and directory paths kept as 'str' so Flyte doesn't manage these static resources
    splat_data_dir: str
    apriori_file: str


@dataclass_json
@dataclass
class MyProxyParameters:
    id: str  # unique identifier for this collection
    job_i_step: int  # step size


@dataclass_json
@dataclass
class MyAprioriConfiguration:
    # Directory with static data directories.
    # TODO: Identify what this data is, possibly split into multiple values if necessary.
    static_data_dir: FlyteDirectory
    # Directory for external dynamic data.
    # TODO: Is this just GEOS data? If so, do we need all of it? Can we preprocess?
    external_data_dir: FlyteDirectory


@dataclass_json
@dataclass
class MyInput:
    level1b_product: FlyteFile
    apriori_config: MyAprioriConfiguration
    proxy_config: MyProxyConfiguration
    proxy_params: MyProxyParameters


def test_dataclass_complex_transform():
    level1b_product = FlyteFile("/tmp/complex/product")
    apriori = MyAprioriConfiguration(static_data_dir=FlyteDirectory("/tmp/apriori/static_data"),
                                     external_data_dir=FlyteDirectory("/tmp/apriori/external_data"))
    proxy_c = MyProxyConfiguration(splat_data_dir="/tmp/proxy_splat", apriori_file="/opt/config/a_file")
    proxy_p = MyProxyParameters(id="pp_id", job_i_step=1)

    my_input = MyInput(
        level1b_product=level1b_product,
        apriori_config=apriori,
        proxy_config=proxy_c,
        proxy_params=proxy_p,
    )

    ctx = FlyteContextManager.current_context()
    literal_type = TypeEngine.to_literal_type(MyInput)
    first_literal = TypeEngine.to_literal(ctx, my_input, MyInput, literal_type)
    print(first_literal)

    converted_back_1 = TypeEngine.to_python_value(ctx, first_literal, MyInput)
    print(converted_back_1)

    second_literal = TypeEngine.to_literal(ctx, converted_back_1, MyInput, literal_type)
    print(second_literal)

    converted_back_2 = TypeEngine.to_python_value(ctx, second_literal, MyInput)
    print(converted_back_2)



