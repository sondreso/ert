import logging
from itertools import combinations as combi
from typing import List, Optional

import pandas as pd

from ert.storage import StorageReader

logger = logging.getLogger(__name__)


class PlotApi:
    def __init__(self, storage):
        self._all_cases: List[dict] = None
        self._timeout = 120
        self._storage: StorageReader = storage
        self._reset_storage_facade()

    def _reset_storage_facade(self):
        self._storage.refresh()

    def _get_case(self, name: str) -> Optional[dict]:
        for e in self._get_all_cases():
            if e["name"] == name:
                return e
        return None

    def _get_all_cases(self) -> List[dict]:
        if self._all_cases is not None:
            return self._all_cases

        self._all_cases = []
        for ensemble in self._storage.ensembles:
            ensemble.name
            self._all_cases.append(
                {
                    "name": ensemble.name,
                    "id": ensemble.id,
                    "hidden": ensemble.name.startswith("."),
                }
            )
        return self._all_cases

    def all_data_type_keys(self) -> List:
        """Returns a list of all the keys except observation keys.

        The keys are a unique set of all keys in the ensembles

        For each key a dict is returned with info about
        the key"""

        all_keys = {}

        all_parameter_sub_keys = set()
        for experiment in self._storage.experiments:
            ensembles = list(experiment.ensembles)
            if len(ensembles) == 0:
                continue
            ensemble = ensembles[0]
            resp_with_obs = [
                obs.attrs["response"] for obs in experiment.observations.values()
            ]
            for key, value in experiment.response_info.items():
                if value["_ert_kind"] == "SummaryConfig":
                    sub_indexes = (
                        ensemble.load_response(
                            key, tuple(range(ensemble.ensemble_size))
                        )
                        .get_index("name")
                        .to_list()
                    )
                    summary_obs = [
                        obs.get_index("name").to_list()[0]
                        for obs in experiment.observations.values()
                        if obs.attrs["response"] == key
                    ]
                    for sub_index in sub_indexes:
                        comb_key = key + ":" + sub_index
                        all_keys[comb_key] = {
                            "key": comb_key,
                            "index_type": "VALUE",
                            "observations": sub_index in summary_obs,
                            "dimensionality": 2,
                            "metadata": {"data_origin": value["_ert_kind"]},
                            "log_scale": comb_key.startswith("LOG10_"),
                        }
                    pass
                else:
                    obs_rep_steps = [
                        obs.get_index("report_step").to_list()[0]
                        for obs in experiment.observations.values()
                        if obs.attrs["response"] == key
                    ]
                    rep_steps = (
                        [0] if value["report_steps"] is None else value["report_steps"]
                    )  # Hvorfor er obs markert med report_step men ikke med i response?
                    for rep_step in rep_steps:
                        all_keys[key] = {
                            "key": f"{key}@{rep_step}",
                            "index_type": "VALUE",
                            "observations": key in resp_with_obs
                            and rep_step in obs_rep_steps,
                            "dimensionality": 2,
                            "metadata": {"data_origin": value["_ert_kind"]},
                            "log_scale": key.startswith("LOG10_"),
                        }
            for key, value in experiment.parameter_info.items():
                sub_indexes = ensemble.load_parameters(key).get_index("names").to_list()
                for sub_index in sub_indexes:
                    all_parameter_sub_keys.add(
                        (key + ":" + sub_index, value["_ert_kind"])
                    )

        for key, kind in sorted(list(all_parameter_sub_keys)):
            all_keys[key] = {
                "key": key,
                "index_type": None,
                "observations": False,
                "dimensionality": 1,
                "metadata": {"data_origin": kind},
                "log_scale": key.startswith("LOG10_"),
            }

        return list(all_keys.values())

    def get_all_cases_not_running(self) -> List:
        """Returns a list of all cases that are not running. For each case a dict with
        info about the case is returned"""
        # Currently, the ensemble information from the storage API does not contain any
        # hint if a case is running or not for now we return all the cases, running or
        # not
        return self._get_all_cases()

    def data_for_key(self, case_name, key) -> pd.DataFrame:
        """Returns a pandas DataFrame with the datapoints for a given key for a given
        case. The row index is the realization number, and the columns are an index
        over the indexes/dates"""

        if key.startswith("LOG10_"):
            key = key[6:]

        case = self._get_case(case_name)

        ensemble = self._storage.get_ensemble(case["id"])
        try:
            main_key, sub_key = (
                key.split(":", maxsplit=1)
                if ":" in key
                else key.split("@", maxsplit=1)
                if "@" in key
                else (key, None)
            )
            data = ensemble.load_response(
                main_key, tuple(range(ensemble.ensemble_size))
            )
            if "@" in key:
                return (
                    data.sel(report_step=int(sub_key))
                    .to_dataframe()
                    .drop(columns="report_step")
                    .unstack("index")
                    .droplevel(0, axis=1)
                )
            else:
                return (
                    data.sel(name=sub_key)
                    .to_dataframe()
                    .drop(columns="name")
                    .unstack("time")
                    .droplevel(0, axis=1)
                )
        except (ValueError, KeyError):
            try:
                tail, head = key.split(":", maxsplit=1)
                data = (
                    ensemble.load_parameters(tail, var="transformed_values")
                    .to_dataframe()
                    .unstack("names")
                    .droplevel(0, axis=1)[[head]]
                )
                data.columns = [0]
                return data
            except (ValueError, KeyError):
                return pd.DataFrame()

    def observations_for_key(self, case_name, key):
        """Returns a pandas DataFrame with the datapoints for a given observation key
        for a given case. The row index is the realization number, and the column index
        is a multi-index with (obs_key, index/date, obs_index), where index/date is
        used to relate the observation to the data point it relates to, and obs_index
        is the index for the observation itself"""

        case = self._get_case(case_name)
        ensemble = self._storage.get_ensemble(case["id"])
        val = None
        main_key, sub_key = (
            key.split(":", maxsplit=1) if ":" in key else key.split("@", maxsplit=1)
        )
        sub_key = int(sub_key) if "@" in key else sub_key
        is_obs_summary = False
        val = None
        for v in ensemble.experiment.observations.values():
            is_summary = "time" in list(v.coords.dims.keys())
            if (
                v.attrs["response"] == main_key
                and sub_key in v.coords["name" if is_summary else "report_step"]
            ):
                is_obs_summary = is_summary
                if val is None:
                    val = v.sel({"name" if is_summary else "report_step": sub_key})
                else:
                    val = val.merge(
                        v.sel({"name" if is_summary else "report_step": sub_key})
                    )

        dict_dataset = val.to_dict()
        data_struct = {
            "STD": dict_dataset["data_vars"]["std"]["data"],
            "OBS": dict_dataset["data_vars"]["observations"]["data"],
            "key_index": val.indexes["time" if is_obs_summary else "index"].to_list(),
        }
        return pd.DataFrame(data_struct).T

    def history_data(self, key, case=None) -> pd.DataFrame:
        """Returns a pandas DataFrame with the data points for the history for a
        given data key, if any.  The row index is the index/date and the column
        index is the key."""

        if ":" in key:
            head, tail = key.split(":", 1)
            if ":" in tail:
                parts = tail.split(":", 1)
                tail = f"{parts[0]}H:{parts[0]}"
            else:
                tail = tail + "H"
            history_key = f"{head}:{tail}"
        else:
            history_key = f"{key}H"

        df = self.data_for_key(case, history_key)

        if not df.empty:
            df = df.T
            # Drop columns with equal data
            duplicate_cols = [
                cc[0] for cc in combi(df.columns, r=2) if (df[cc[0]] == df[cc[1]]).all()
            ]
            return df.drop(columns=duplicate_cols)

        return pd.DataFrame()
