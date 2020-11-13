from prefect import Flow, task, resource_manager, Parameter
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import logging


@task
def PrintClient(client, parameter):
    print(client, parameter)
    return client


@resource_manager
class ManageResource:
    def __init__(self):
        return

    def setup(self):
        return "Create a local dask cluster"

    def cleanup(self):
        return "Cleanup the local dask cluster"


with Flow("Resource Manager with Parameter") as flow:
    parameter = Parameter("resource_to_manage")

    with ManageResource() as client:
        PrintClient(client=client, parameter=parameter)

flow.register(project_name="PROJECT: Schematics")