from inflect import engine
from prefect import Flow, task
from random import uniform

@task
def dynamic_task(i):
    print(engine(i) + " Task")

with Flow("Dynamic Tasks") as DynamicTasksFlow:
    j = 0
    for i in range(50):
        k = uniform(0, j)
        dynamic_task(i, task_args=dict(name=f"Task-{i}"), upstream_tasks=[f"Task-{k}"])
        j += 1
