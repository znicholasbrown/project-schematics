from prefect import Flow, Task, Parameter
from prefect.schedules import IntervalSchedule
from prefect.engine.results import PrefectResult
from prefect.environments.storage import GitHub

import time
from datetime import timedelta


class Add(Task, result=PrefectResult()):
    def run(self, x, y=1):
        self.logger.info(
            """
            x: {x}
            y: {y}
        """
        )
        time.sleep(5)
        return


default_range = list(range(10))

with Flow("Single Mapped Task") as flow:
    parameter = Parameter("no_tasks", default=default_range)
    prefect_result = Add().map(parameter)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/PrefectResult.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
