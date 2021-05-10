import asyncio
import logging
import threading
from contextlib import contextmanager
import pickle
from typing import Set
import cloudevents.exceptions
from http import HTTPStatus

import cloudpickle

import ert_shared.ensemble_evaluator.entity.identifiers as identifiers
import ert_shared.ensemble_evaluator.monitor as ee_monitor
import websockets
from websockets.legacy.server import WebSocketServerProtocol
from async_generator import asynccontextmanager
from cloudevents.http import from_json, to_json
from cloudevents.http.event import CloudEvent
from ert_shared.ensemble_evaluator.dispatch import Dispatcher, Batcher
from ert_shared.ensemble_evaluator.entity import serialization
from ert_shared.ensemble_evaluator.entity.snapshot import (
    PartialSnapshot,
    Snapshot,
    Job,
    Realization,
    SnapshotDict,
    Step,
)
from ert_shared.status.entity.state import (
    ENSEMBLE_STATE_CANCELLED,
    ENSEMBLE_STATE_FAILED,
    ENSEMBLE_STATE_STARTED,
    ENSEMBLE_STATE_STOPPED,
    JOB_STATE_START,
    REALIZATION_STATE_WAITING,
    STEP_STATE_UNKNOWN,
)

logger = logging.getLogger(__name__)


class EnsembleEvaluator:
    def __init__(self, ensemble, config, iter_, ee_id: str = "0"):
        # Without information on the iteration, the events emitted from the
        # evaluator are ambiguous. In the future, an experiment authority* will
        # "own" the evaluators and can add iteration information to events they
        # emit. In the mean time, it is added here.
        # * https://github.com/equinor/ert/issues/1250
        self._iter = iter_
        self._ee_id = ee_id

        self._config = config
        self._ensemble = ensemble

        self._loop = asyncio.new_event_loop()

        self._done = self._loop.create_future()

        self._clients: Set[WebSocketServerProtocol] = set()
        self._dispatchers_connected: asyncio.Queue[None] = asyncio.Queue(
            loop=self._loop
        )

        self._batcher = Batcher(timeout=2, loop=self._loop)
        self._dispatcher = Dispatcher(batcher=self._batcher)

        self._register_event_handlers(dispatcher=self._dispatcher)

        self._snapshot = self.create_snapshot(ensemble)
        self._event_index = 1
        self._result = None

        self._ws_thread = threading.Thread(
            name="ert_ee_run_server", target=self._run_server, args=(self._loop,)
        )

    def _register_event_handlers(self, dispatcher):
        dispatcher.register_event_handler(identifiers.EVGROUP_FM_ALL, batching=True)(
            self._fm_handler
        )
        dispatcher.register_event_handler(identifiers.EVTYPE_ENSEMBLE_STOPPED)(
            self._ensemble_stopped_handler
        )
        dispatcher.register_event_handler(identifiers.EVTYPE_ENSEMBLE_STARTED)(
            self._ensemble_started_handler
        )
        dispatcher.register_event_handler(identifiers.EVTYPE_ENSEMBLE_CANCELLED)(
            self._ensemble_cancelled_handler
        )
        dispatcher.register_event_handler(identifiers.EVTYPE_ENSEMBLE_FAILED)(
            self._ensemble_failed_handler
        )

    @staticmethod
    def create_snapshot(ensemble):
        reals = {}
        for real in ensemble.get_active_reals():
            reals[str(real.get_iens())] = Realization(
                active=True,
                status=REALIZATION_STATE_WAITING,
            )
            for step in real.get_steps():
                reals[str(real.get_iens())].steps[str(step.get_id())] = Step(
                    status=STEP_STATE_UNKNOWN
                )
                for job in step.get_jobs():
                    reals[str(real.get_iens())].steps[str(step.get_id())].jobs[
                        str(job.get_id())
                    ] = Job(
                        status=JOB_STATE_START,
                        data={},
                        name=job.get_name(),
                    )
        top = SnapshotDict(
            reals=reals,
            status=ENSEMBLE_STATE_STARTED,
            metadata=ensemble.get_metadata(),
        )

        return Snapshot(top.dict())

    async def _fm_handler(self, events):
        snapshot_mutate_event = PartialSnapshot(self._snapshot)
        for event in events:
            snapshot_mutate_event.from_cloudevent(event)
        await self._send_snapshot_update(snapshot_mutate_event)

    async def _ensemble_stopped_handler(self, event):
        self._result = event.data
        if self._snapshot.get_status() != ENSEMBLE_STATE_FAILED:
            snapshot_mutate_event = PartialSnapshot(self._snapshot).from_cloudevent(
                event
            )
            await self._send_snapshot_update(snapshot_mutate_event)

    async def _ensemble_started_handler(self, event):
        if self._snapshot.get_status() != ENSEMBLE_STATE_FAILED:
            snapshot_mutate_event = PartialSnapshot(self._snapshot).from_cloudevent(
                event
            )
            await self._send_snapshot_update(snapshot_mutate_event)

    async def _ensemble_cancelled_handler(self, event):
        if self._snapshot.get_status() != ENSEMBLE_STATE_FAILED:
            snapshot_mutate_event = PartialSnapshot(self._snapshot).from_cloudevent(
                event
            )
            await self._send_snapshot_update(snapshot_mutate_event)
            self._stop()

    async def _ensemble_failed_handler(self, event):
        if self._snapshot.get_status() not in [
            ENSEMBLE_STATE_STOPPED,
            ENSEMBLE_STATE_CANCELLED,
        ]:
            snapshot_mutate_event = PartialSnapshot(self._snapshot).from_cloudevent(
                event
            )
            await self._send_snapshot_update(snapshot_mutate_event)

    async def _send_snapshot_update(self, snapshot_mutate_event):
        self._snapshot.merge_event(snapshot_mutate_event)
        out_cloudevent = CloudEvent(
            {
                "type": identifiers.EVTYPE_EE_SNAPSHOT_UPDATE,
                "source": f"/ert/ee/{self._ee_id}",
                "id": self.event_index(),
            },
            snapshot_mutate_event.to_dict(),
        )
        out_cloudevent.data["iter"] = self._iter
        out_msg = to_json(
            out_cloudevent, data_marshaller=serialization.evaluator_marshaller
        ).decode()
        if out_msg and self._clients:
            await asyncio.wait([client.send(out_msg) for client in self._clients])

    @staticmethod
    def create_snapshot_msg(ee_id, iter_, snapshot, event_index):
        data = snapshot.to_dict()
        data["iter"] = iter_
        out_cloudevent = CloudEvent(
            {
                "type": identifiers.EVTYPE_EE_SNAPSHOT,
                "source": f"/ert/ee/{ee_id}",
                "id": event_index,
            },
            data,
        )
        return to_json(
            out_cloudevent, data_marshaller=serialization.evaluator_marshaller
        ).decode()

    @contextmanager
    def store_client(self, websocket):
        self._clients.add(websocket)
        yield
        try:
            self._clients.remove(websocket)
        except KeyError:
            logger.debug(
                f"Tried removing client {websocket.remote_address} twice. Likely the client was removed after sending a signal."
            )

    async def handle_client(self, websocket, path):
        with self.store_client(websocket):
            message = self.create_snapshot_msg(
                self._ee_id, self._iter, self._snapshot, self.event_index()
            )
            await websocket.send(message)

            async for message in websocket:
                client_event = from_json(
                    message, data_unmarshaller=serialization.evaluator_unmarshaller
                )
                logger.debug(f"got message from client: {client_event}")
                if client_event["type"] == identifiers.EVTYPE_EE_USER_CANCEL:
                    logger.debug(f"Client {websocket.remote_address} asked to cancel.")
                    if self._ensemble.is_cancellable():
                        # The evaluator will stop after the ensemble has
                        # indicated it has been cancelled.
                        self._ensemble.cancel()
                    else:
                        self._stop()

                if client_event["type"] == identifiers.EVTYPE_EE_USER_DONE:
                    logger.debug(f"Client {websocket.remote_address} signalled done.")
                    self._stop()

                # NOTE: due to how the monitor is implemented, a monitor that
                # signals will open a connection for each signal and
                # immediately exit after signalling. Consequently, it should be
                # harmless to remove the client from the pool.
                # If https://github.com/equinor/ert/issues/1538 is solved, then
                # this necessarily needs to change.
                self._clients.remove(websocket)

    @asynccontextmanager
    async def count_dispatcher(self):
        await self._dispatchers_connected.put(None)
        yield
        await self._dispatchers_connected.get()
        self._dispatchers_connected.task_done()

    async def handle_dispatch(self, websocket, path):
        async with self.count_dispatcher():
            async for msg in websocket:
                try:
                    event = from_json(
                        msg, data_unmarshaller=serialization.evaluator_unmarshaller
                    )
                except cloudevents.exceptions.DataUnmarshallerError:
                    event = from_json(msg, data_unmarshaller=lambda x: pickle.loads(x))
                if self._get_ee_id(event["source"]) != self._ee_id:
                    logger.info(
                        f"Got event from evaluator {self._get_ee_id(event['source'])} with source {event['source']}, ignoring since I am {self._ee_id}"
                    )
                    continue
                await self._dispatcher.handle_event(event)
                if event["type"] in [
                    identifiers.EVTYPE_ENSEMBLE_STOPPED,
                    identifiers.EVTYPE_ENSEMBLE_FAILED,
                ]:
                    return

    async def handle_result(self, websocket, path):
        if self._result is None:
            event = CloudEvent(
                {
                    "type": identifiers.EVTYPE_EE_RESULT_NOT_READY,
                    "source": f"/ert/ee/{self._ee_id}",
                },
            )
        else:
            event = CloudEvent(
                {
                    "type": identifiers.EVTYPE_EE_RESULT,
                    "source": f"/ert/ee/{self._ee_id}",
                    "datacontenttype": "application/octet-stream",
                },
                cloudpickle.dumps(self._result),
            )
        await websocket.send(to_json(event))

    async def connection_handler(self, websocket, path):
        elements = path.split("/")
        if elements[1] == "client":
            await self.handle_client(websocket, path)
        elif elements[1] == "dispatch":
            await self.handle_dispatch(websocket, path)
        elif elements[1] == "result":
            await self.handle_result(websocket, path)
        else:
            logger.info(f"Connection attempt to unknown path: {path}.")

    async def process_request(self, path, request_headers):
        if request_headers.get("token") != self._config.token:
            return HTTPStatus.UNAUTHORIZED, {}, b""
        if path == "/healthcheck":
            return HTTPStatus.OK, {}, b""

    async def evaluator_server(self, done):
        async with websockets.serve(
            self.connection_handler,
            sock=self._config.get_socket(),
            ssl=self._config.get_server_ssl_context(),
            process_request=self.process_request,
            max_queue=None,
            max_size=2 ** 26,
        ):
            await done
            logger.debug("Got done signal.")
            # give NFS adaptors and Queue adaptors some time to read/send last events
            await self._dispatchers_connected.join()
            await self._batcher.join()
            message = self.terminate_message()
            if self._clients:
                await asyncio.wait([client.send(message) for client in self._clients])
            logger.debug("Sent terminated to clients.")

        logger.debug("Async server exiting.")

    def _run_server(self, loop):
        loop.run_until_complete(self.evaluator_server(self._done))
        logger.debug("Server thread exiting.")

    def terminate_message(self):
        out_cloudevent = CloudEvent(
            {
                "type": identifiers.EVTYPE_EE_TERMINATED,
                "source": f"/ert/ee/{self._ee_id}",
                "id": str(self.event_index()),
            }
        )
        message = to_json(
            out_cloudevent, data_marshaller=serialization.evaluator_marshaller
        ).decode()
        return message

    def event_index(self):
        index = self._event_index
        self._event_index += 1
        return index

    def run(self) -> ee_monitor._Monitor:
        self._ws_thread.start()
        self._ensemble.evaluate(self._config, self._ee_id)
        return ee_monitor.create(
            self._config.host,
            self._config.port,
            self._config.protocol,
            self._config.cert,
            self._config.token,
        )

    def _stop(self):
        if not self._done.done():
            self._done.set_result(None)

    def stop(self):
        self._loop.call_soon_threadsafe(self._stop)
        self._ws_thread.join()

    def get_successful_realizations(self):
        return self._snapshot.get_successful_realizations()

    def run_and_get_successful_realizations(self):
        with self.run() as mon:
            for _ in mon.track():
                pass
        return self.get_successful_realizations()

    @staticmethod
    def _get_ee_id(source) -> str:
        # the ee_id will be found at /ert/ee/ee_id/...
        return source.split("/")[3]
