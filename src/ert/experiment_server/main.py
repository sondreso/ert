import queue

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel, Field

from ert.ensemble_evaluator import EvaluatorServerConfig
from ert.run_models.model_factory import create_model
from ert.run_models.base_run_model import BaseRunModel, StatusEvents
from ert.gui.simulation.ensemble_experiment_panel import Arguments as EnsembleExperimentArguments
from ert.gui.simulation.ensemble_smoother_panel import Arguments as EnsembleSmootherArguments
from ert.storage import open_storage

from typing import Dict, Union
import uuid

from ert.config import ErtConfig, QueueSystem


class Experiment(BaseModel):
    args: Union[EnsembleExperimentArguments, EnsembleSmootherArguments] = Field(..., discriminator='mode')
    ert_config: ErtConfig

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

experiments : Dict[str, BaseRunModel]= {}

def run_experiment(experiment_id:str, evaluator_server_config: EvaluatorServerConfig):
    experiments[experiment_id].start_simulations_thread(evaluator_server_config=evaluator_server_config)

@app.post("/experiments/")
async def submit_experiment(experiment: Experiment, background_tasks: BackgroundTasks):
    storage = open_storage(experiment.ert_config.ens_path, "w")
    status_queue: queue.SimpleQueue[StatusEvents] = queue.SimpleQueue()
    try:
        model = create_model(
            experiment.ert_config,
            storage,
            experiment.args,
            status_queue,
        )
    except ValueError as e:
        return HTTPException(status_code=404, detail=f"{experiment.args.mode} was not valid, failed with: {e}")

    if experiment.args.port_range is None and model.queue_system == QueueSystem.LOCAL:
        experiment.args.port_range = range(49152, 51819)

    evaluator_server_config = EvaluatorServerConfig(custom_port_range=experiment.args.port_range)

    experiment_id = str(uuid.uuid4())
    experiments[experiment_id] = model

    background_tasks.add_task(run_experiment, experiment_id, evaluator_server_config=evaluator_server_config)
    return {"message": "Experiment Started", "experiment_id": experiment_id}
