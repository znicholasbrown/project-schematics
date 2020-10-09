import prefect
from prefect import Flow, Task, task
import time
import random
from datetime import timedelta, timezone, datetime
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GitHub
from prefect.engine.executors import LocalDaskExecutor
from prefect.environments import LocalEnvironment
from prefect.engine.results import LocalResult


class Version(Task):
    def run(self):
        self.logger.debug(f"Running on Prefect v{prefect.__version__}")
        return


class Root(Task):
    def run(self):
        self.logger.debug("Root running...")
        time.sleep(random.randint(1, 5))
        self.logger.debug("Root complete.")
        return list(range(5))


class Node(Task):
    def run(self):
        self.logger.debug(f"{self.name} running...")
        time.sleep(random.randint(1, 5))
        if random.random() > 0.98:
            raise ValueError(f"{self.name} failed :(")
        else:
            self.logger.debug(f"{self.name} complete.")
            return list(range(5))


schedule = IntervalSchedule(interval=timedelta(minutes=1))
with Flow("Mapped - Local Dask Executor", schedule=schedule) as flow:
    version = Version()

    root = Root(checkpoint=False)(upstream_tasks=[version])
    node1_1 = Node(name="Node 1_1", checkpoint=False).map(upstream_tasks=[root])
    node1_2 = Node(name="Node 1_2", checkpoint=False).map(upstream_tasks=[root])
    node2_1 = Node(name="Node 2_1", checkpoint=False).map(
        upstream_tasks=[node1_1, node1_2]
    )
    node3_1 = Node(name="Node 3_1", checkpoint=False).map(upstream_tasks=[node2_1])
    node3_2 = Node(name="Node 3_2", checkpoint=False).map(upstream_tasks=[node2_1])
    node4_1 = Node(name="Node 4_1", checkpoint=False).map(
        upstream_tasks=[node3_1, node3_2]
    )

flow.environment = LocalEnvironment(
    labels=[],
    executor=LocalDaskExecutor(scheduler="threads", num_workers=6),
)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/LocalDaskExecutor.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
