from dataclasses import dataclass
from typing import List

from dataclasses_json import dataclass_json

from flytekit.core.context_manager import ExecutionState, FlyteContextManager, Image, ImageConfig, SerializationSettings
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.type_engine import TypeEngine
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile


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


level1b_product = FlyteFile("/tmp/complex/product")
apriori = MyAprioriConfiguration(
    static_data_dir=FlyteDirectory("/tmp/apriori/static_data"),
    external_data_dir=FlyteDirectory("/tmp/apriori/external_data"),
)
proxy_c = MyProxyConfiguration(splat_data_dir="/tmp/proxy_splat", apriori_file="/opt/config/a_file")
proxy_p = MyProxyParameters(id="pp_id", job_i_step=1)

my_input = MyInput(
    level1b_product=level1b_product,
    apriori_config=apriori,
    proxy_config=proxy_c,
    proxy_params=proxy_p,
)

my_input2 = MyInput(
    level1b_product=level1b_product,
    apriori_config=apriori,
    proxy_config=proxy_c,
    proxy_params=proxy_p,
)


def test_dataclass_complex_transform():
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

    input_list = [my_input, my_input2]
    input_list_type = TypeEngine.to_literal_type(List[MyInput])
    literal_list_1 = TypeEngine.to_literal(ctx, input_list, List[MyInput], input_list_type)
    print(literal_list_1)


def test_two():
    @dynamic
    def dt1(a: List[MyInput]) -> List[FlyteFile]:
        x = []
        for aa in a:
            x.append(aa.level1b_product)

        return x

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(
            SerializationSettings(
                project="test_proj",
                domain="test_domain",
                version="abc",
                image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
                env={},
            )
        )
    ) as ctx:
        with FlyteContextManager.with_context(
            ctx.with_execution_state(
                ctx.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION,
                    additional_context={
                        "dynamic_addl_distro": "s3://my-s3-bucket/fast/123",
                        "dynamic_dest_dir": "/User/flyte/workflows",
                    },
                )
            )
        ) as ctx:
            input_literal_map = TypeEngine.dict_to_literal_map(
                ctx, d={"a": [my_input, my_input2]}, guessed_python_types={"a": List[MyInput]}
            )
            dynamic_job_spec = dt1.dispatch_execute(ctx, input_literal_map)
            print(dynamic_job_spec)
