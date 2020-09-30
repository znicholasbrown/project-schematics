from prefect import Task, Flow
from prefect.environments.storage import GitHub

with Flow("400 Tasks") as flow:
    for i in range(400):
        flow.add_task(Task(name=f"{i}"))


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/400 Tasks.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
