import prefect
from prefect import Flow, Task, task, Parameter
import time
import random
from datetime import timedelta, timezone, datetime
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GitHub
from prefect.engine.results import LocalResult


class Node(Task):
    def run(self, sleep):
        self.logger.info(f"{self.name} running for {sleep} seconds...")
        time.sleep(sleep)


schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow("Configurable Sleepy Flow", schedule=schedule) as flow:
    sleep = Parameter("seconds", default=240)
    node1_1 = Node(name="Sleepy Node")(sleep=sleep)

flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/Configurable Sleepy Flow.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)

flow.register(project_name="PROJECT: Schematics")
