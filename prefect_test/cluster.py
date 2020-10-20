from dask_jobqueue.lsf import LSFJob, LSFCluster
from dask.distributed import Client
import time


async def _eq_submit_job(self, script_filename):
    with open(script_filename) as fh:
        lines = fh.readlines()[1:]
    lines = [
        line.strip() if "#BSUB" not in line else line[5:].strip() for line in lines
    ]
    piped_cmd = [self.submit_command + " ".join(lines)]
    return self._call(piped_cmd, shell=True)


def start_standalone_client():
    LSFJob._submit_job = _eq_submit_job

    cluster = LSFCluster(
        queue="mr",
        project="test",
        cores=1,
        memory="1GB",
        use_stdin=True,
        silence_logs="debug",
    )
    cluster.scale(2)
    client = Client(cluster)

    while True:
        time.sleep(1)


if __name__ == "__main__":
    start_standalone_client()
