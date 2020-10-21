from prefect import Flow, task
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import logging

logging.basicConfig(datefmt="")

os.environ["TZ"] = "US/Eastern"


@task
def DoSpyStuff():
    return "🕵🏽  Spy stuff done 🕵🏽"


agent_1_clock = CronClock(cron="* * * * *", labels=["Derek Flint"])
agent_2_clock = CronClock(cron="* * * * *", labels=["Evenlyn Salt"])
agent_3_clock = CronClock(cron="* * * * *", labels=["George Smiley"])

schedule = Schedule(
    clocks=[
        agent_1_clock,
        agent_2_clock,
        agent_3_clock,
    ]
)

with Flow("Mission: Possible (with labels)") as flow:
    DoSpyStuff()


flow.run()