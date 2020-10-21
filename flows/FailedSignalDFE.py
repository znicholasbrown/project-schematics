import prefect
from prefect import Flow, task, Parameter
from prefect.schedules import IntervalSchedule
from prefect.engine import FlowRunner
from datetime import timedelta


@task
def return_list(val):
    return list(range(0, val))


@task
def parse_value(val):
    if val % 2 != 0:
        raise ValueError("Value is not even!")

    return val


@task(trigger=prefect.triggers.any_failed)
def catch_error(val):
    print(f"Do something with this value error: {val}")


schedule = IntervalSchedule(interval=timedelta(minutes=1))
with Flow("Raise error on Odd", schedule=schedule) as flow:
    my_param = Parameter("param")
    my_list = return_list(val=my_param)

    def_list = parse_value.map(my_list)

    catch_error.map(def_list)

params = [1, 2, 3, 4, 5]

for p in params:
    FlowRunner(flow=flow).initialize_run(parameters={"param": p})