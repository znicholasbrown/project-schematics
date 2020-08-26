from prefect import Flow, Task, Parameter
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GitHub

import time
from datetime import timedelta


class MapHandler(Task):
    def run(self, item):
        self.logger.info(item)
        time.sleep()
        return


schedule = IntervalSchedule(interval=timedelta(minutes=30))
with Flow("Single Mapped Task", schedule=schedule) as flow:
    parameter = Parameter("no_tasks", default=range(30))

    MapHandler().map(parameter)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics", path="flows/LocalDaskExecutor.py",
)


flow.register(project_name="PROJECT: Schematics")
