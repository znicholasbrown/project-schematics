from typing import List
import prefect
from prefect import Flow, task, Parameter
from prefect.tasks.prefect import StartFlowRun
from prefect.storage import GitHub


@task
def return_input(input: any):
    return input


flow_storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/flow_orchestrator.py",
    secrets=["GITHUB_AUTH_TOKEN"],
    ref="master",
)

with Flow("Orchestration Dependency A") as flow_a:
    input = Parameter("input", default="Hello, World!")
    return_input(input=input)


flow_a.storage = flow_storage
flow_a.register(project_name="PROJECT: Schematics")

with Flow("Orchestration Dependency B") as flow_b:
    input = Parameter("input", default="Goodbye, World!")
    return_input(input=input)

flow_b.storage = flow_storage
flow_b.register(project_name="PROJECT: Schematics")


@task
def get_id(input):
    id = input.state.message.split(" ", 1)[0]
    logger = prefect.context.get("logger")
    logger.debug(id)
    return id


@task
def log_all_results(results: List[any]):
    logger = prefect.context.get("logger")

    for result in results:
        logger.info(type(result))


with Flow("Orchestration Orchestrator") as flow_c:
    a = StartFlowRun(
        project_name="PROJECT: Schematics",
        parameters={"input": "¡Hola, mundo!"},
        wait=True,
    )(flow_name="Orchestration Dependency A", run_name="ODEP-A")

    # get_id(a)

    b = StartFlowRun(
        project_name="PROJECT: Schematics",
        parameters={"input": "¡Adiós, mundo!"},
        wait=True,
    )(flow_name="Orchestration Dependency B", run_name="ODEP-B")

    # get_id(b)

    # log_all_results(results=[a, b])

flow_c.storage = flow_storage
# flow_c.run()
flow_c.register(project_name="PROJECT: Schematics")
