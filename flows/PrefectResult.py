from prefect import Flow, Task, Parameter
from prefect.schedules import IntervalSchedule
from prefect.engine.results import PrefectResult
from prefect.environments.storage import GitHub

import time
from datetime import timedelta


class Add(Task):
    def __init__(
        self,
        name=None,
        slug=None,
        tags=None,
        max_retries=None,
        retry_delay=None,
        timeout=None,
        trigger=None,
        skip_on_upstream_skip=True,
        cache_for=None,
        cache_validator=None,
        cache_key=None,
        checkpoint=None,
        result_handler=None,
        state_handlers=None,
        on_failure=None,
        log_stdout=False,
        result=PrefectResult(),
        target=None,
    ):
        super().__init__(
            name=name,
            slug=slug,
            tags=tags,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            trigger=trigger,
            skip_on_upstream_skip=skip_on_upstream_skip,
            cache_for=cache_for,
            cache_validator=cache_validator,
            cache_key=cache_key,
            checkpoint=checkpoint,
            result_handler=result_handler,
            state_handlers=state_handlers,
            on_failure=on_failure,
            log_stdout=log_stdout,
            result=result,
            target=target,
        )

    def run(self, x, y=1):
        self.logger.info(
            """
            x: {x}
            y: {y}
        """
        )
        time.sleep(5)
        return


default_range = list(range(10))

with Flow("Single Mapped Task") as flow:
    parameter = Parameter("no_tasks", default=default_range)
    prefect_result = Add().map(parameter)


flow.storage = GitHub(
    repo="znicholasbrown/project-schematics",
    path="flows/PrefectResult.py",
    secrets=["GITHUB_AUTH_TOKEN"],
)


flow.register(project_name="PROJECT: Schematics")
