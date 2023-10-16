import functools
import logging
import os
from pathlib import Path
import warnings
import webbrowser
from signal import SIG_DFL, SIGINT, signal
from typing import List, Optional, Tuple, cast

from PyQt5.QtGui import QIcon
from qtpy.QtCore import QDir, QLocale, QSize, Qt
from qtpy.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from ert.config import ConfigValidationError, ConfigWarning, ErtConfig
from ert.enkf_main import EnKFMain
from ert.gui.about_dialog import AboutDialog
from ert.gui.ertwidgets import SuggestorMessage, SummaryPanel
from ert.gui.main_window import ErtMainWindow
from ert.gui.simulation import SimulationPanel
from ert.gui.tools.event_viewer import (
    EventViewerTool,
    GUILogHandler,
    add_gui_log_handler,
)
from ert.gui.tools.export import ExportTool
from ert.gui.tools.load_results import LoadResultsTool
from ert.gui.tools.manage_cases import ManageCasesTool
from ert.gui.tools.plot import PlotTool
from ert.gui.tools.plugins import PluginHandler, PluginsTool
from ert.gui.tools.run_analysis import RunAnalysisTool
from ert.gui.tools.workflows import WorkflowsTool
from ert.libres_facade import LibresFacade
from ert.namespace import Namespace
from ert.services import StorageService
from ert.shared.plugins.plugin_manager import ErtPluginManager
from ert.storage import StorageReader, open_storage
from ert.storage.local_storage import local_storage_set_ert_config


def show_window(app, window):
    window.show()
    window.activateWindow()
    window.raise_()
    return app.exec_()


def run_gui(args: Namespace, plugin_manager: Optional[ErtPluginManager] = None):
    # Replace Python's exception handler for SIGINT with the system default.
    #
    # Python's SIGINT handler is the one that raises KeyboardInterrupt. This is
    # okay normally (if a bit ugly), but when control is given to Qt this
    # exception handler will either get deadlocked because Python never gets
    # control back, or gets eaten by Qt because it ignores exceptions that
    # happen in Qt slots.
    signal(SIGINT, SIG_DFL)

    QDir.addSearchPath(
        "img", os.path.join(os.path.dirname(__file__), "resources/gui/img")
    )

    app = QApplication([])  # Early so that QT is initialized before other imports
    app.setWindowIcon(QIcon("img:application/window_icon_cutout"))
    mode = "r" if args.read_only else "w"  # TODO does this have any effect??
    with add_gui_log_handler() as log_handler:
        ert, ert_config, error_messages, config_warnings, deprecations = _check_config(
            args.config
        )

        if ert is None:
            suggester_window = _setup_suggester(
                error_messages,
                config_warnings,
                deprecations,
                plugin_manager=plugin_manager,
            )
            return show_window(app, suggester_window)

        with open_storage(ert_config.ens_path, mode="r") as storage, open_storage(
            ert_config.ens_path, mode="w"
        ) as wstorage:
            _main_window = _setup_main_window(ert, args, storage, log_handler)
            _main_window.notifier.set_storage(wstorage)
            show_window(
                app,
                _setup_suggester(
                    error_messages,
                    config_warnings,
                    deprecations,
                    _main_window,
                    plugin_manager=plugin_manager,
                )
                if deprecations or config_warnings
                else _main_window,
            )


def _check_config(
    config_file: Path,
) -> Tuple[Optional[EnKFMain], Optional[ErtConfig], List[str], List[str], List[str]]:
    # Create logger inside function to make sure all handlers have been added to
    # the root-logger.
    logger = logging.getLogger(__name__)
    error_messages = []
    all_warnings = []
    config_warnings = []
    ert_config = None
    ert = None

    with warnings.catch_warnings(record=True) as all_warnings:
        try:
            _check_locale()
            ert_dir = os.path.abspath(os.path.dirname(config_file))
            os.chdir(ert_dir)
            # Changing current working directory means we need to update
            # the config file to be the base name of the original config
            config_file = os.path.basename(config_file)
            ert_config = ErtConfig.from_file(config_file)
            local_storage_set_ert_config(ert_config)
            ert = EnKFMain(ert_config)
        except ConfigValidationError as error:
            error_messages += error.messages()
            logger.info("Error in config file shown in gui: '%s'", str(error))
        finally:
            config_warnings = [
                str(w.message)
                for w in all_warnings
                if w.category == ConfigWarning
                and not cast(ConfigWarning, w.message).info.is_deprecation
            ]
            deprecations = [
                str(w.message)
                for w in all_warnings
                if w.category == ConfigWarning
                and cast(ConfigWarning, w.message).info.is_deprecation
            ]
    for job in ert_config.forward_model_list:
        logger.info("Config contains forward model job %s", job.name)

    for wm in all_warnings:
        if wm.category != ConfigWarning:
            logger.warning(str(wm.message))
    for msg in deprecations:
        logger.info("Suggestion shown in gui '%s'", msg)
    for msg in config_warnings:
        logger.info("Warning shown in gui '%s'", msg)
    return ert, ert_config, error_messages, config_warnings, deprecations


def _check_locale():
    # There seems to be a setlocale() call deep down in the initialization of
    # QApplication, if the user has set the LC_NUMERIC environment variables to
    # a locale with decimalpoint different from "." the application will fail
    # hard quite quickly.
    current_locale = QLocale()
    decimal_point = str(current_locale.decimalPoint())
    if decimal_point != ".":
        msg = f"""You are using a locale with decimalpoint: '{decimal_point}'
the ert application is written with the assumption that '.' is  used as
decimalpoint, and chances are that something will break if you continue with
this locale. It is highly recommended that you set the decimalpoint to '.'
using one of the environment variables 'LANG', LC_ALL', or 'LC_NUMERIC' to
either the 'C' locale or alternatively a locale which uses '.' as
decimalpoint.\n"""  # noqa
        warnings.warn(msg, category=ConfigWarning, stacklevel=1)


def _clicked_help_button(menu_label: str, link: str):
    logger = logging.getLogger(__name__)
    logger.info(f"Pressed help button {menu_label}")
    webbrowser.open(link)


def _clicked_about_button(about_dialog):
    logger = logging.getLogger(__name__)
    logger.info("Pressed help button About")
    about_dialog.show()


def _setup_suggester(
    errors,
    warning_msgs,
    suggestions,
    ert_window=None,
    plugin_manager: Optional[ErtPluginManager] = None,
):
    container = QWidget()
    if ert_window is not None:
        container.notifier = ert_window.notifier
    container.setWindowTitle("Some problems detected")
    container_layout = QVBoxLayout()

    help_button_frame = QFrame()
    help_buttons_layout = QHBoxLayout()
    help_button_frame.setLayout(help_buttons_layout)

    button_size = QSize(-1, -1)
    helpbuttons = []

    help_label = QLabel("Help:")
    help_buttons_layout.addWidget(help_label)
    help_links = plugin_manager.get_help_links() if plugin_manager else {}

    for menu_label, link in help_links.items():
        button = QPushButton(menu_label)
        button.setObjectName(menu_label)
        button.clicked.connect(
            functools.partial(_clicked_help_button, menu_label, link)
        )
        helpbuttons.append(button)
        help_buttons_layout.addWidget(button)

    about_button = QPushButton("About")
    about_button.setObjectName("about_button")
    helpbuttons.append(about_button)
    help_buttons_layout.addWidget(about_button)

    diag = AboutDialog(container)
    about_button.clicked.connect(lambda: _clicked_about_button(diag))

    for b in helpbuttons:
        b.adjustSize()
        if b.size().width() > button_size.width():
            button_size = b.size()

    for b in helpbuttons:
        b.setMinimumSize(button_size)

    help_buttons_layout.insertStretch(-1, -1)

    container_layout.addWidget(help_button_frame)

    suggest_msgs = QWidget()
    buttons = QWidget()
    suggest_layout = QVBoxLayout()
    buttons_layout = QHBoxLayout()

    text = ""
    for msg in errors:
        text += msg + "\n"
        suggest_layout.addWidget(SuggestorMessage.error_msg(msg))
    for msg in warning_msgs:
        text += msg + "\n"
        suggest_layout.addWidget(SuggestorMessage.warning_msg(msg))
    for msg in suggestions:
        text += msg + "\n"
        suggest_layout.addWidget(SuggestorMessage.deprecation_msg(msg))

    suggest_layout.addStretch()
    suggest_msgs.setLayout(suggest_layout)
    scroll = QScrollArea()
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    scroll.setWidgetResizable(True)
    scroll.setWidget(suggest_msgs)

    def copy_text():
        QApplication.clipboard().setText(text)

    def run_pressed():
        ert_window.show()
        ert_window.activateWindow()
        ert_window.raise_()
        ert_window.adjustSize()
        container.close()

    run = QPushButton("Open ERT")
    give_up = QPushButton("Exit")
    copy = QPushButton("Copy messages")

    run.setObjectName("run_ert_button")
    run.setEnabled(ert_window is not None)
    run.pressed.connect(run_pressed)
    copy.pressed.connect(copy_text)
    give_up.pressed.connect(container.close)

    buttons_layout.addWidget(copy)
    buttons_layout.insertStretch(-1, -1)
    buttons_layout.addWidget(run)
    buttons_layout.addWidget(give_up)

    buttons.setLayout(buttons_layout)
    container_layout.addWidget(scroll)
    container_layout.addWidget(buttons)
    container.setLayout(container_layout)
    container.resize(800, 600)
    return container


def _setup_main_window(
    ert: EnKFMain,
    args: Namespace,
    storage: StorageReader,
    log_handler: GUILogHandler,
    plugin_manager: Optional[ErtPluginManager] = None,
):
    # window reference must be kept until app.exec returns:
    facade = LibresFacade(ert)
    config_file = args.config
    config = ert.ert_config
    window = ErtMainWindow(config_file, plugin_manager)

    plot_tool = PlotTool(config_file, storage, window)
    window.setWidget(
        SimulationPanel(ert, window.notifier, config_file, plot_tool.trigger)
    )
    plugin_handler = PluginHandler(
        ert,
        window.notifier,
        [wfj for wfj in ert.ert_config.workflow_jobs.values() if wfj.is_plugin()],
        window,
    )

    window.addDock(
        "Configuration summary", SummaryPanel(ert), area=Qt.BottomDockWidgetArea
    )
    window.addTool(plot_tool)
    window.addTool(ExportTool(ert, window.notifier))
    window.addTool(WorkflowsTool(ert, window.notifier))
    window.addTool(
        ManageCasesTool(config, window.notifier, config.model_config.num_realizations)
    )
    window.addTool(PluginsTool(plugin_handler, window.notifier))
    window.addTool(RunAnalysisTool(ert, window.notifier))
    window.addTool(LoadResultsTool(facade, window.notifier))
    event_viewer = EventViewerTool(log_handler)
    window.addTool(event_viewer)
    window.close_signal.connect(event_viewer.close_wnd)
    window.adjustSize()
    return window
