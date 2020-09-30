import random
from datetime import timedelta
from prefect import task, Flow


@task
def generate_random_list():
    n = random.randint(5, 15)
    return list(range(n))


@task
def randomly_fail():
    x = random.random()
    if x > 0.7:
        raise ValueError(f"{x} failed :(")


with Flow("Randomly Fail") as flow:
    final = randomly_fail.map(upstream_tasks=[generate_random_list])


flow.register(project_name="PROJECT: Schematics")