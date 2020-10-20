from prefect import task, Flow
from prefect.engine.executors import DaskExecutor
from dask_jobqueue.lsf import LSFJob
import prefect
from cluster import _eq_submit_job


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.warning("Hello, Equinor!")


@task
def add(x, y=1, meta=None):
    if meta is None:
        meta = {}
    meta2 = meta.copy()
    meta2[(x, y)] = "Called :D"
    meta[(x, y)] = "Called :D"
    return {"sum": x + y, "result2": 3, "meta": meta, "meta2": meta2}


def main():
    cluster_kwargs = {
        "queue": "mr",
        "project": None,
        "cores": 1,
        "memory": "1GB",
        "use_stdin": True,
        "n_workers": 2,
        "silence_logs": "debug",
    }
    executor = DaskExecutor(
        cluster_class="dask_jobqueue.LSFCluster",
        cluster_kwargs=cluster_kwargs,
        debug=True,
    )

    with Flow("Test LSF Flow") as flow:
        say_hello()

        first_result = add(1, y=2)
        second_result = add(
            first_result["sum"], first_result["result2"], first_result["meta"]
        )

    state = flow.run(executor=executor)

    assert state.is_successful()

    first_task_state = state.result[first_result]

    print(first_result)
    print(second_result)
    print(state.result[first_result].result)
    print(state.result[second_result].result)
    # flow.visualize()


if __name__ == "__main__":
    LSFJob._submit_job = _eq_submit_job
    main()
