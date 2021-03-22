from typing import Tuple

from prefect import Flow, task, flatten


@task
def task_1() -> Tuple[list, str]:
    # generate your data and the variable of interest
    data = [1, 2, 3]
    variable = "some variable"
    return (data, variable)


@task
def task_2(data):
    print(data)
    return data


@task
def task_3(data, variable):
    # do something with variable
    print(data)
    print(variable)


with Flow("flow") as flow:
    *data, variable = task_1()
    result = task_2.map(data)
    task_3(flatten(result), variable)

flow.run()