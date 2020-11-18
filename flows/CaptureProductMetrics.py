import prefect
from prefect import Flow, Task, task
import time
import random
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GitHub
from prefect.engine.executors import LocalDaskExecutor
from prefect.environments import LocalEnvironment


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
        time.sleep(random.randint(1, 5))
        if random.random() > 0.98:
            raise ValueError(f"{self.name} failed :(")
        else:
            self.logger.info(f"{self.name} complete.")
            return list(range(5))


storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/CaptureProductMetrics.py",
    secrets=["GITHUB_AUTH_TOKEN"],
    ref="master",
)

environment = LocalEnvironment(
    labels=[],
    executor=LocalDaskExecutor(scheduler="threads", num_workers=6),
)

schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow(
    "Capture Product Metrics",
    schedule=schedule,
    storage=storage,
    environment=environment,
) as flow:
    version = Version()

    root = Root(checkpoint=False)(upstream_tasks=[version])

    node1_1 = Node(name="Fetch Users")(upstream_tasks=[root])
    node1_2 = Node(name="Fetch Extra Params")(upstream_tasks=[root])
    node1_3 = Node(name="Test on Staging Data")(upstream_tasks=[root])
    node1_4 = Node(name="Anonymize Before Transfer")(upstream_tasks=[root])
    node1_5 = Node(name="Create Downstream Nodes")(upstream_tasks=[root])
    node1_6 = Node(name="Filter Leads")(upstream_tasks=[root])

    node2_1 = Node(name="Merge Metrics")(upstream_tasks=[node1_1, node1_2])
    node2_2 = Node(name="Catch Unfiltered")(upstream_tasks=[node1_5, node1_6])

    node3_1 = Node(name="Post Leads")(upstream_tasks=[node2_1])
    node3_2 = Node(name="Normalize Users")(upstream_tasks=[node2_1])
    node3_3 = Node(name="Transfer to BigQuery")(upstream_tasks=[node2_1])
    node3_4 = Node(name="Extrapolate Vertices")(upstream_tasks=[node2_2])
    node3_5 = Node(name="Elevate References")(upstream_tasks=[node2_2])
    node3_6 = Node(name="Generate Wildcard Dashboards")(upstream_tasks=[node2_2])

    node4_1 = Node(name="Register Criteria")(upstream_tasks=[node3_1, node3_2])
    node4_2 = Node(name="Incremement Next Schedule")(upstream_tasks=[node3_1, node3_3])
    node4_3 = Node(name="Generate Reports")(upstream_tasks=[node3_2, node3_4])
    node4_4 = Node(name="Generate New Leads")(upstream_tasks=[node3_1, node3_2])
    node4_5 = Node(name="Upload to S3")(upstream_tasks=[node3_5, node3_3])
    node4_6 = Node(name="Notify Slack")(upstream_tasks=[node3_6, node3_5])


flow.register(project_name="Product Flows")
# flow.run()