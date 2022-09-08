#  Copyright (C) 2011  Equinor ASA, Norway.
#
#  The file 'gert_main.py' is part of ERT - Ensemble based Reservoir Tool.
#
#  ERT is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  ERT is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.
#
#  See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html>
#  for more details.
import argparse
import logging
import os
import sys

from qtpy import QtGui
from qtpy.QtCore import QLocale, Qt
from qtpy.QtGui import QColor, QFont, QPalette
from qtpy.QtWidgets import QApplication, QMessageBox

from ert._c_wrappers.enkf import EnKFMain, ResConfig
from ert.gui.ertnotifier import ErtNotifier
from ert.gui.ertwidgets import SummaryPanel, resourceIcon
from ert.gui.main_window import GertMainWindow
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
from ert.shared.services import Storage


def run_gui(args):
    app = QApplication([])  # Early so that QT is initialized before other imports
    QtGui.QFontDatabase.addApplicationFont('fonts/Equinor-Regular.otf')
    QtGui.QFontDatabase.addApplicationFont('fonts/Equinor-Bold.otf')
    font = QFont()
    font.setFamily(u"Equinor")
    font.setPointSize(10)
    app.setFont(font)


    eq_background_default = QColor(255, 255, 255)
    eq_background_light = QColor(247, 247, 247)
    eq_background_medium = QColor(220, 220, 220)

    eq_text_default = QColor(61, 61, 61)
    eq_text_primary_white = QColor(255, 255, 255)

    eq_interactive_resting = QColor(0, 112, 121)
    eq_interactive_hover = QColor(0, 79, 85)

    palette = QPalette()
    palette.setColor(QPalette.Window, eq_background_default)
    palette.setColor(QPalette.WindowText, eq_text_default)
    palette.setColor(QPalette.Base, eq_background_light)
    palette.setColor(QPalette.AlternateBase, eq_background_medium)

    # palette.setColor(QPalette.ToolTipBase, Qt.white)
    # palette.setColor(QPalette.ToolTipText, eq_text_default)

    palette.setColor(QPalette.Text, eq_text_default)
    palette.setColor(QPalette.HighlightedText, eq_text_default)

    # palette.setColor(QPalette.Button, eq_interactive_resting)
    # palette.setColor(QPalette.ButtonText, eq_text_primary_white)
    # palette.setColor(QPalette.BrightText, eq_text_default)

    # palette.setColor(QPalette.Link, eq_interactive_resting)

    # palette.setColor(QPalette.Highlight, eq_interactive_hover)

    # palette.setColor(QPalette.Light, eq_interactive_resting)
    # palette.setColor(QPalette.Midlight, eq_interactive_resting)
    # palette.setColor(QPalette.Dark, eq_interactive_resting)
    # palette.setColor(QPalette.Mid, eq_interactive_resting)
    # palette.setColor(QPalette.Shadow, eq_interactive_resting)

    app.setPalette(palette)
    eq_style_sheet = """
    QPushButton, QAbstractButton {
        background-color: #007079;
        color: #FFFFFF;
        padding-top: 6px;
        padding-bottom: 6px;
        padding-left: 16px;
        padding-right: 16px;
        border-width: 1px;
        border-color: #007079;
        border-radius: 4px;
    }
    QPushButton:hover, QAbstractButton:hover {
        background-color: #004F55;
    }
    QComboBox {
        background-color: #F7F7F7;
        border-style: none;
        border-bottom: 1px solid #3D3D3D;
        padding: 4px;
    }
    QComboBox:item {
        background-color: #FFFFFF;
    }

    QComboBox:item:selected {
        background-color: #DCDCDC;
    }

    QComboBox:item::hover {
        background-color: #DCDCDC;
    }

    QComboBox::down-arrow {
    }

    """
    app.setStyleSheet(eq_style_sheet)

    app.setWindowIcon(resourceIcon("application/window_icon_cutout"))
    res_config = ResConfig(args.config)

    # Create logger inside function to make sure all handlers have been added to
    # the root-logger.
    logger = logging.getLogger(__name__)
    logger.info(
        "Logging forward model jobs",
        extra={
            "workflow_jobs": str(res_config.model_config.getForwardModel().joblist())
        },
    )

    os.chdir(res_config.config_path)
    # Changing current working directory means we need to update the config file to
    # be the base name of the original config
    args.config = os.path.basename(args.config)
    ert = EnKFMain(res_config)
    with Storage.connect_or_start_server(
        res_config=os.path.basename(args.config)
    ), add_gui_log_handler() as log_handler:
        notifier = ErtNotifier(args.config)
        # window reference must be kept until app.exec returns:
        window = _start_window(ert, notifier, args, log_handler)  # noqa
        return app.exec_()


def _start_window(
    ert: EnKFMain,
    notifier: ErtNotifier,
    args: argparse.Namespace,
    log_handler: GUILogHandler,
):

    _check_locale()

    window = _setup_main_window(ert, notifier, args, log_handler)
    window.show()
    window.activateWindow()
    window.raise_()

    if not ert.have_observations():
        QMessageBox.warning(
            window,
            "Warning!",
            "No observations loaded. Model update algorithms disabled!",
        )

    return window


def _check_locale():
    # There seems to be a setlocale() call deep down in the initialization of
    # QApplication, if the user has set the LC_NUMERIC environment variables to
    # a locale with decimalpoint different from "." the application will fail
    # hard quite quickly.
    current_locale = QLocale()
    decimal_point = str(current_locale.decimalPoint())
    if decimal_point != ".":
        msg = f"""
** WARNING: You are using a locale with decimalpoint: '{decimal_point}' - the ert application is
            written with the assumption that '.' is  used as decimalpoint, and chances
            are that something will break if you continue with this locale. It is highly
            recommended that you set the decimalpoint to '.' using one of the environment
            variables 'LANG', LC_ALL', or 'LC_NUMERIC' to either the 'C' locale or
            alternatively a locale which uses '.' as decimalpoint.\n"""  # noqa

        sys.stderr.write(msg)


def _setup_main_window(
    ert: EnKFMain,
    notifier: ErtNotifier,
    args: argparse.Namespace,
    log_handler: GUILogHandler,
):
    facade = LibresFacade(ert)
    config_file = args.config
    window = GertMainWindow(config_file)
    window.setWidget(SimulationPanel(ert, notifier, config_file))
    plugin_handler = PluginHandler(ert, ert.getWorkflowList().getPluginJobs(), window)

    window.addDock(
        "Configuration summary", SummaryPanel(ert), area=Qt.BottomDockWidgetArea
    )
    window.addTool(PlotTool(config_file))
    window.addTool(ExportTool(ert))
    window.addTool(WorkflowsTool(ert, notifier))
    window.addTool(ManageCasesTool(ert, notifier))
    window.addTool(PluginsTool(plugin_handler, notifier))
    window.addTool(RunAnalysisTool(ert, notifier))
    window.addTool(LoadResultsTool(facade))
    event_viewer = EventViewerTool(log_handler)
    window.addTool(event_viewer)
    window.close_signal.connect(event_viewer.close_wnd)
    window.adjustSize()
    return window
