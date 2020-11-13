import prefect
from prefect import Flow, Task, Parameter


class OptedTask(Task):
    def __init__(self, opts=None, **kwargs):
        # This lets you instantitate opts at build time
        self.opts = opts
        super().__init__(kwargs)

    def get_host_from_kv(self):
        return self.opts["kwopts"]["host"]


class Useful(OptedTask):
    def run(self, opts):
        # This lets you instantiate opts at runtime
        self.opts = opts
        host = self.get_host_from_kv()
        return host


with Flow("Query a single host") as flow:
    opts_param = Parameter("opts", default={"kwopts": {"host": "host"}})

    apt = Useful()

    result = apt(opts=opts_param)

flow.run()