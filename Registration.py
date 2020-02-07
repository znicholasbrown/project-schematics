import importlib
import sys
from prefect.environments.storage import Docker

def RegisterFlow(module):
    f = importlib.import_module(f"flows.{module}")

    f.flow.storage = Docker(
        base_image="python:3.7",
        registry_url="znicholasbrown",
        image_name=module.lower(),
        image_tag=module.lower(),
    )

    f.flow.register(project_name="PROJECT: Schematics")

RegisterFlow(sys.argv[1])