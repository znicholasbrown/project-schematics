import prefect
from prefect import Flow, Task
import time
import random
from datetime import timedelta, timezone, datetime
from prefect.schedules import IntervalSchedule

class Version(Task):
    def run(self):
        self.logger.info(f"Running on Prefect v{prefect.__version__}")
        return

class Root(Task):
    def run(self):
        self.logger.info('Root running...')
        time.sleep(5)
        self.logger.info('Root complete.')
        return list(range(random.randint(1, 10)))

class Node(Task):
    def run(self):
        self.logger.info(f'{self.name} running...')
        time.sleep(5)
        self.logger.info(f'{self.name} complete.')
        return list(range(random.randint(1, 10)))

schedule = IntervalSchedule(interval=timedelta(minutes=4))
with Flow("Mapped 4 Minute Interval", schedule=schedule) as flow:
    version = Version()

    root = Root()(upstream_tasks=[version])
    node1_1 = Node(name="Node 1_1").map(upstream_tasks=[root])
    node1_2 = Node(name="Node 1_2").map(upstream_tasks=[root])
    node2_1 = Node(name="Node 2_1").map(upstream_tasks=[node1_1, node1_2])
    node3_1 = Node(name="Node 3_1").map(upstream_tasks=[node2_1])
    node3_2 = Node(name="Node 3_2").map(upstream_tasks=[node2_1])
    node4_1 = Node(name="Node 4_1").map(upstream_tasks=[node3_1, node3_2])