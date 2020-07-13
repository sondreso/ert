import inspect
import os

from jinja2 import Template
import res

from ert_shared.plugins.plugin_manager import hook_implementation
from ert_shared.plugins.plugin_response import plugin_response


def _resolve_ert_share_path():

    share_path = os.path.realpath(
        os.path.join(os.path.dirname(inspect.getfile(res)), "../../../../share/ert")
    )

    if not os.path.isdir(share_path):
        share_path = os.path.realpath(
            os.path.join(os.path.dirname(inspect.getfile(res)), "../../share/ert")
        )
    return share_path


def _get_jobs_from_directories(directories):
    share_path = _resolve_ert_share_path()
    directories = list(
        [
            Template(l).render(ERT_SHARE_PATH=share_path, ERT_UI_MODE="gui")
            for l in directories
        ]
    )

    all_files = []
    for directory in directories:
        all_files.extend(
            [
                os.path.join(directory, f)
                for f in os.listdir(directory)
                if os.path.isfile(os.path.join(directory, f))
            ]
        )
    return {os.path.basename(path): path for path in all_files}


def _get_file_content_if_exists(file_name, default=""):
    if os.path.isfile(file_name):
        with open(file_name) as fh:
            return fh.read()
    return default


def _get_job_category(job_name):
    if "FILE" in job_name or "DIR" in job_name or "SYMLINK" in job_name:
        return "utility.file_system"

    if job_name in {"ECLIPSE100", "ECLIPSE300", "FLOW"}:
        return "simulators.reservoir"

    if job_name == "RMS":
        return "modeling.reservoir"

    if job_name == "TEMPLATE_RENDER":
        return "utility.templating"

    return "other"


@hook_implementation
@plugin_response(plugin_name="ert")
def installable_jobs():
    directories = [
        "{{ERT_SHARE_PATH}}/forward-models/shell",
        "{{ERT_SHARE_PATH}}/forward-models/res",
        "{{ERT_SHARE_PATH}}/forward-models/templating",
        "{{ERT_SHARE_PATH}}/forward-models/old_style",
    ]
    return _get_jobs_from_directories(directories)


@hook_implementation
@plugin_response(plugin_name="ert")
def job_documentation(job_name):
    ert_jobs = set(installable_jobs().data.keys())
    if job_name not in ert_jobs:
        return None

    doc_folder = "{}/forward-models/docs".format(_resolve_ert_share_path())

    description_file = os.path.join(
        doc_folder, "description", "{}.rst".format(job_name)
    )
    description = _get_file_content_if_exists(description_file)

    examples_file = os.path.join(doc_folder, "examples", "{}.rst".format(job_name))
    examples = _get_file_content_if_exists(examples_file)

    return {
        "description": description,
        "examples": examples,
        "category": _get_job_category(job_name),
    }


@hook_implementation
@plugin_response(plugin_name="ert")
def installable_workflow_jobs():
    directories = [
        "{{ERT_SHARE_PATH}}/workflows/jobs/internal/config",
        "{{ERT_SHARE_PATH}}/workflows/jobs/internal-{{ERT_UI_MODE}}/config",
    ]
    return _get_jobs_from_directories(directories)
