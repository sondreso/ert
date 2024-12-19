import json
import os
import ssl
from functools import partial
from pathlib import Path
from unittest.mock import patch

from ropt.enums import OptimizerExitCode
from seba_sqlite.snapshot import SebaSnapshot

from everest.config import EverestConfig, ServerConfig
from everest.detached import ServerStatus, everserver_status
from everest.detached.jobs import everserver
from everest.simulator import JOB_FAILURE, JOB_SUCCESS
from everest.strings import OPT_FAILURE_REALIZATIONS, SIM_PROGRESS_ENDPOINT


def configure_everserver_logger(*args, **kwargs):
    """Mock exception raised"""
    raise Exception("Configuring logger failed")


def check_status(*args, **kwargs):
    everest_server_status_path = str(Path(args[0]).parent / "status")
    status = everserver_status(everest_server_status_path)
    assert status["status"] == kwargs["status"]


def fail_optimization(self, from_ropt=False):
    # Patch start_optimization to raise a failed optimization callback. Also
    # call the provided simulation callback, which has access to the shared_data
    # variable in the eversever main function. Patch that callback to modify
    # shared_data (see set_shared_status() below).
    self._sim_callback(None)
    if from_ropt:
        self._exit_code = OptimizerExitCode.TOO_FEW_REALIZATIONS
        return OptimizerExitCode.TOO_FEW_REALIZATIONS

    raise Exception("Failed optimization")


def set_shared_status(*args, progress, shared_data):
    # Patch _sim_monitor with this to access the shared_data variable in the
    # everserver main function.
    failed = len(
        [job for queue in progress for job in queue if job["status"] == JOB_FAILURE]
    )

    shared_data[SIM_PROGRESS_ENDPOINT] = {
        "status": {"failed": failed},
        "progress": progress,
    }


def test_certificate_generation(copy_math_func_test_data_to_tmp):
    config = EverestConfig.load_file("config_minimal.yml")
    cert, key, pw = everserver._generate_certificate(
        ServerConfig.get_certificate_dir(config.output_dir)
    )

    # check that files are written
    assert os.path.exists(cert)
    assert os.path.exists(key)

    # check certificate is readable
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(cert, key, pw)  # raise on error


def test_hostfile_storage(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    host_file_path = "detach/.session/host_file"

    expected_result = {
        "host": "hostname.1.2.3",
        "port": "5000",
        "cert": "/a/b/c.cert",
        "auth": "1234",
    }
    everserver._write_hostfile(host_file_path, **expected_result)
    assert os.path.exists(host_file_path)
    with open(host_file_path, encoding="utf-8") as f:
        result = json.load(f)
    assert result == expected_result


@patch("sys.argv", ["name", "--config-file", "config_minimal.yml"])
@patch(
    "everest.detached.jobs.everserver._configure_loggers",
    side_effect=configure_everserver_logger,
)
def test_everserver_status_failure(_1, copy_math_func_test_data_to_tmp):
    config_file = "config_minimal.yml"
    config = EverestConfig.load_file(config_file)
    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    assert status["status"] == ServerStatus.failed
    assert "Exception: Configuring logger failed" in status["message"]


@patch("sys.argv", ["name", "--config-file", "config_minimal.yml"])
@patch(
    "ert.run_models.everest_run_model.EverestRunModel.run_experiment",
    autospec=True,
    side_effect=lambda self, evaluator_server_config, restart=False: check_status(
        ServerConfig.get_hostfile_path(self.everest_config.output_dir),
        status=ServerStatus.running,
    ),
)
def test_everserver_status_running_complete(
    _1, mock_server, copy_math_func_test_data_to_tmp
):
    config_file = "config_minimal.yml"
    config = EverestConfig.load_file(config_file)
    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    assert status["status"] == ServerStatus.completed
    assert status["message"] == "Optimization completed."


@patch("sys.argv", ["name", "--config-file", "config_minimal.yml"])
@patch(
    "ert.run_models.everest_run_model.EverestRunModel.run_experiment",
    autospec=True,
    side_effect=lambda self, evaluator_server_config, restart=False: fail_optimization(
        self, from_ropt=True
    ),
)
@patch(
    "everest.detached.jobs.everserver._sim_monitor",
    side_effect=partial(
        set_shared_status,
        progress=[
            [
                {"name": "job1", "status": JOB_FAILURE, "error": "job 1 error 1"},
                {"name": "job1", "status": JOB_FAILURE, "error": "job 1 error 2"},
            ],
            [
                {"name": "job2", "status": JOB_SUCCESS, "error": ""},
                {"name": "job2", "status": JOB_FAILURE, "error": "job 2 error 1"},
            ],
        ],
    ),
)
def test_everserver_status_failed_job(
    _1, _2, mock_server, copy_math_func_test_data_to_tmp
):
    config_file = "config_minimal.yml"
    config = EverestConfig.load_file(config_file)
    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    # The server should fail and store a user-friendly message.
    assert status["status"] == ServerStatus.failed
    assert OPT_FAILURE_REALIZATIONS in status["message"]
    assert "job1 Failed with: job 1 error 1" in status["message"]
    assert "job1 Failed with: job 1 error 2" in status["message"]
    assert "job2 Failed with: job 2 error 1" in status["message"]


@patch("sys.argv", ["name", "--config-file", "config_minimal.yml"])
@patch(
    "ert.run_models.everest_run_model.EverestRunModel.run_experiment",
    autospec=True,
    side_effect=lambda self, evaluator_server_config, restart=False: fail_optimization(
        self, from_ropt=False
    ),
)
@patch(
    "everest.detached.jobs.everserver._sim_monitor",
    side_effect=partial(set_shared_status, progress=[]),
)
def test_everserver_status_exception(
    _1, _2, mock_server, copy_math_func_test_data_to_tmp
):
    config_file = "config_minimal.yml"
    config = EverestConfig.load_file(config_file)
    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    # The server should fail, and store the exception that
    # start_optimization raised.
    assert status["status"] == ServerStatus.failed
    assert "Exception: Failed optimization" in status["message"]


@patch("sys.argv", ["name", "--config-file", "config_one_batch.yml"])
@patch(
    "everest.detached.jobs.everserver._sim_monitor",
    side_effect=partial(set_shared_status, progress=[]),
)
def test_everserver_status_max_batch_num(
    _1, mock_server, copy_math_func_test_data_to_tmp
):
    config_file = "config_one_batch.yml"
    config = EverestConfig.load_file(config_file)
    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    # The server should complete without error.
    assert status["status"] == ServerStatus.completed

    # Check that there is only one batch.
    snapshot = SebaSnapshot(config.optimization_output_dir).get_snapshot(
        filter_out_gradient=False, batches=None
    )
    assert {data.batch for data in snapshot.simulation_data} == {0}


@patch("sys.argv", ["name", "--config-file", "config_minimal.yml"])
def test_everserver_status_contains_max_runtime_failure(
    mock_server, change_to_tmpdir, min_config
):
    config_file = "config_minimal.yml"

    Path("SLEEP_job").write_text("EXECUTABLE sleep", encoding="utf-8")
    min_config["simulator"] = {"max_runtime": 2}
    min_config["forward_model"] = ["sleep 5"]
    min_config["install_jobs"] = [{"name": "sleep", "source": "SLEEP_job"}]

    config = EverestConfig(**min_config)
    config.dump(config_file)

    everserver.main()
    status = everserver_status(
        ServerConfig.get_everserver_status_path(config.output_dir)
    )

    assert status["status"] == ServerStatus.failed
    print(status["message"])
    assert (
        "sleep Failed with: The run is cancelled due to reaching MAX_RUNTIME"
        in status["message"]
    )
