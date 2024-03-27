from typing import Optional

import jsonpickle
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.langchain.task import _get_langchain_instance

from flytekit.extend.backend.base_agent import AgentRegistry, Resource, SyncAgentBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class LangChainAgent(SyncAgentBase):
    """
    It is used to run Airflow tasks. It is registered as an agent in the AgentRegistry.
    There are three kinds of Airflow tasks: AirflowOperator, AirflowSensor, and AirflowHook.

    Sensor is always invoked in get method. Calling get method to check if the certain condition is met.
    For example, FileSensor is used to check if the file exists. If file doesn't exist, agent returns
    RUNNING status, otherwise, it returns SUCCEEDED status.

    Hook is a high-level interface to an external platform that lets you quickly and easily talk to
     them without having to write low-level code that hits their API or uses special libraries. For example,
     SlackHook is used to send messages to Slack. Therefore, Hooks are also invoked in get method.
    Note: There is no running state for Hook. It is either successful or failed.

    Operator is invoked in create method. Flytekit will always set deferrable to True for Operator. Therefore,
    `operator.execute` will always raise TaskDeferred exception after job is submitted. In the get method,
    we create a trigger to check if the job is finished.
    Note: some of the operators are not deferrable. For example, BeamRunJavaPipelineOperator, BeamRunPythonPipelineOperator.
     In this case, those operators will be converted to AirflowContainerTask and executed in the pod.
    """

    name = "LangChain Agent"

    def __init__(self):
        super().__init__(task_type_name="langchain")

    async def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> Resource:
        # print(task_template.interface.inputs)
        # print(task_template.interface.outputs)
        langchain_obj = jsonpickle.decode(task_template.custom["task_config_pkl"])
        # langchain_openai.chat_models.base.ChatOpenAI
        langchain_instance = _get_langchain_instance(langchain_obj)
        print("v1 langchain_instance:", langchain_instance)
        model = langchain_instance(**langchain_obj.parameters)
        print("v2 langchain_instance:", model)
        # print("langchain_instance.invoke:", model.invoke("hi"))
        # print("langchain_obj:", langchain_obj)
        # print("langchain_instance:", langchain_instance)
        # print("inputs:", inputs)
        # ctx = FlyteContextManager.current_context()
        # input_python_value = TypeEngine.literal_map_to_kwargs(ctx, inputs, {"input": Any})
        # print("input_python_value:", type(input_python_value))
        # print("input_python_value:", input_python_value)
        # message = input_python_value.get("input")
        # print("message:", message)
        # message = langchain_obj.invoke(message)
        # outputs = {"output": message}
        # custom = task_template.custom
        # custom["chatgpt_config"]["messages"] = [{"role": "user", "content": message}]
        # client = openai.AsyncOpenAI(
        #     organization=custom["openai_organization"],
        #     api_key=get_agent_secret(secret_key=OPENAI_API_KEY),
        # )

        # logger = logging.getLogger("httpx")
        # logger.setLevel(logging.WARNING)

        # completion = await asyncio.wait_for(client.chat.completions.create(**custom["chatgpt_config"]), TIMEOUT_SECONDS)
        # message = completion.choices[0].message.content
        # outputs = {"o0": message}

        # return Resource(phase=TaskExecution.SUCCEEDED,)
        return Resource(phase=TaskExecution.SUCCEEDED, outputs={"o0": model})


AgentRegistry.register(LangChainAgent())
