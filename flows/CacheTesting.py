from prefect import Flow, task, context
from datetime import timedelta
import ipdb


@task(cache_for=timedelta(minutes=0.5))
def fetch_data():
    return ["hello!"]


def trigger_fn(upstream_states) -> bool:
    ipdb.set_trace()
    return upstream_states


@task(trigger=trigger_fn)
def insert_data(data):
    print(data)


with Flow("Some Flow") as flow:
    data = fetch_data()

    insert_data(data)

flow.run()