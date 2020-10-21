import random
from datetime import timedelta, time
import prefect
from prefect import Task, Flow
from prefect.environments.storage import GitHub
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefect.engine.executors import LocalDaskExecutor
from prefect.environments import LocalEnvironment

from prefect.schedules.filters import is_day_of_week, is_month_start


class Version(Task):
    def run(self):
        self.logger.info(f"Running on Prefect v{prefect.__version__}")
        return


class Root(Task):
    def run(self):
        self.logger.info("Root running...")
        time.sleep(1)
        self.logger.info("Root complete.")
        return list(range(random.randint(1, 10)))


class Node(Task):
    def run(self):
        self.logger.info(f"{self.name} running...")
        time.sleep(1)
        if random.random() >= 1:
            raise ValueError(f"{self.name} failed :(")
        else:
            self.logger.info(f"{self.name} complete.")
            return list(range(random.randint(1, 25)))


schedule = Schedule(
    clocks=[IntervalClock(timedelta(hours=12))],
    filters=[is_month_start, is_day_of_week(2)],
)
with Flow("Multi-level Parallel Mapping", schedule=schedule) as flow:
    version = Version()

    root = Root()(upstream_tasks=[version])
    node1_1 = Node(name="Node 1_1").map(upstream_tasks=[root])
    node1_2 = Node(name="Node 1_2").map(upstream_tasks=[root])
    node2_1 = Node(name="Node 2_1").map(upstream_tasks=[node1_1, node1_2])
    node3_1 = Node(name="Node 3_1").map(upstream_tasks=[node2_1])
    node3_2 = Node(name="Node 3_2").map(upstream_tasks=[node2_1])
    node4_1 = Node(name="Node 4_1").map(upstream_tasks=[node3_1, node3_2])

flow.environment = LocalEnvironment(
    labels=[],
    executor=LocalDaskExecutor(scheduler="threads", num_workers=6),
)

flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/FilteredSchedule.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
