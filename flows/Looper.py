import prefect
from prefect import Flow, task, Parameter
from prefect.engine.signals import LOOP
from prefect.engine.results import LocalResult
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock


@task()
def log_output(result):
    logger = prefect.context.get("logger")
    logger.info(result)


@task(
    result=LocalResult(
        dir="/Users/nicholasbrown/Projects/flows/project-schematics/flows/results",
        location=lambda **kwargs: f"{prefect.context.get('task_loop_count')}.prefect",
    )
)
def loop_test():
    n = prefect.context.get("task_loop_result", 0)

    print(n)

    if n > 5:
        return n

    raise LOOP(result=n + 1)


with Flow("Postgres -> BigQuery") as flow:
    x = loop_test()
    log_output(x)

flow.run()