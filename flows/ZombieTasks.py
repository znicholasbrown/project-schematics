from prefect import Flow, Task, Parameter
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GitHub

import time
from datetime import timedelta
import sys


class MapHandler(Task):
    def run(self, item):
        self.logger.info(item)
        time.sleep(45)
        sys.exit()


default_range = list(range(5))

schedule = IntervalSchedule(interval=timedelta(minutes=1.5))
with Flow("Local Zombie Tasks", schedule=schedule) as flow:
    parameter = Parameter("no_tasks", default=default_range)

    MapHandler().map(parameter)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/ZombieTasks.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
