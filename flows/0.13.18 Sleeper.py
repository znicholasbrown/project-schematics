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
from prefect.run_configs import KubernetesRun


class Version(Task):
    def run(self):
        self.logger.info(f"Running on Prefect v{prefect.__version__}")
        return


class Root(Task):
    def run(self):
        self.logger.info("Root running...")
        time.sleep(random.randint(1, 5))
        self.logger.info("Root complete.")
        return list(range(5))


class Node(Task):
    def run(self):
        self.logger.info(f"{self.name} running...")
        time.sleep(random.randint(1, 240))
        self.logger.info(f"{self.name} complete.")
        return list(range(5))


schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow("0.13.18 Sleeper", schedule=schedule) as flow:
    version = Version()

    root = Root(checkpoint=False)(upstream_tasks=[version])

    node1_1 = Node(name="Node 1_1", checkpoint=False).map(root)
    node1_2 = Node(name="Node 1_2", checkpoint=False).map(root)
    node1_3 = Node(name="Node 1_3", checkpoint=False).map(root)
    node1_4 = Node(name="Node 1_4", checkpoint=False).map(root)
    node1_5 = Node(name="Node 1_5", checkpoint=False).map(root)
    node1_6 = Node(name="Node 1_6", checkpoint=False).map(root)

    node2_1 = Node(name="Node 2_1", checkpoint=False).map(
        node1_1, upstream_tasks=[node1_1, node1_2]
    )
    node2_2 = Node(name="Node 2_2", checkpoint=False).map(
        node1_1, upstream_tasks=[node1_1, node1_2]
    )
    node2_3 = Node(name="Node 2_3", checkpoint=False).map(
        node1_1, upstream_tasks=[node1_1, node1_2]
    )
    node2_4 = Node(name="Node 2_4", checkpoint=False).map(
        node1_1, upstream_tasks=[node1_1, node1_2]
    )
    node2_5 = Node(name="Node 2_5", checkpoint=False).map(
        node1_2, upstream_tasks=[node1_1, node1_2]
    )
    node2_6 = Node(name="Node 2_6", checkpoint=False).map(
        node1_2, upstream_tasks=[node1_1, node1_2]
    )

    node3_1 = Node(name="Node 3_1", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )
    node3_2 = Node(name="Node 3_2", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )
    node3_3 = Node(name="Node 3_3", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )
    node3_4 = Node(name="Node 3_4", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )
    node3_5 = Node(name="Node 3_5", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )
    node3_6 = Node(name="Node 3_6", checkpoint=False).map(
        node2_1, upstream_tasks=[node2_1]
    )

    node4_1 = Node(name="Node 4_1", checkpoint=False).map(
        node3_1, upstream_tasks=[node3_1, node3_2]
    )
    node4_2 = Node(name="Node 4_2", checkpoint=False).map(
        node3_1, upstream_tasks=[node3_1, node3_2]
    )
    node4_3 = Node(name="Node 4_3", checkpoint=False).map(
        node3_2, upstream_tasks=[node3_1, node3_2]
    )
    node4_4 = Node(name="Node 4_4", checkpoint=False).map(
        node3_2, upstream_tasks=[node3_1, node3_2]
    )
    node4_5 = Node(name="Node 4_5", checkpoint=False).map(
        node3_2, upstream_tasks=[node3_1, node3_2]
    )
    node4_6 = Node(name="Node 4_6", checkpoint=False).map(
        node3_2, upstream_tasks=[node3_1, node3_2]
    )

flow.environment = LocalEnvironment(
    labels=[],
    executor=LocalDaskExecutor(scheduler="threads", num_workers=6),
)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/0.13.18 Sleeper.py",
    secrets=["GITHUB_AUTH_TOKEN"],
    ref="master",
)

flow.register(project_name="PROJECT: Schematics")
