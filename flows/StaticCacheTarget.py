from prefect.engine.results import LocalResult
from prefect import task, Flow


@task(target="add_target.txt", checkpoint=True, result=LocalResult(dir="."))
def add(x, y=1):
    ret = x + y
    return ret


@task
def print_value(x):
    print(x)


with Flow("my handled flow!", result=LocalResult()) as flow:
    first_result = add(1, y=2)
    second_result = add(x=first_result, y=100)
    print_value(second_result)

    
flow.run()
