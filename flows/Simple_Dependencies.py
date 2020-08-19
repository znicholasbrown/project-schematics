import prefect
from prefect import Flow, Task, task
import time
from datetime import timedelta, timezone, datetime
from prefect.schedules import IntervalSchedule


class Root(Task):
    def run(self):
        self.logger.warning(f"Running on Prefect v{prefect.__version__}")
        print("Root running...")
        time.sleep(5)
        print("Root complete.")
        return


class Node1_1(Task):
    def run(self):
        print("Node 1_1 running...")
        time.sleep(5)
        print("Node 1_1 complete.")
        return


class Node1_2(Task):
    def run(self):
        print("Node 1_2 running...")
        time.sleep(5)
        print("Node 1_2 complete.")
        return


schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow("Simple Dependencies", schedule=schedule) as flow:
    root = Root()
    node1_1 = Node1_1()
    node1_2 = Node1_2()

    node1_1(upstream_tasks=[root])
    node1_2(upstream_tasks=[root, node1_1])
