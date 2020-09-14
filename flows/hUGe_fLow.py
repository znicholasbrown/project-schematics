from prefect import Task, Flow
from prefect.environments.storage import GitHub

with Flow("hUGe fLow") as flow:
    for i in range(2000):
        flow.add_task(Task(name=f"{i}"))


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/hUGe_fLow.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="Dev Straining")
