################################################################################
##
## BY: WANDERSON M.PIMENTA
## PROJECT MADE WITH: Qt Designer and PySide2
## V: 1.0.0
##
## This project can be used freely for all uses, as long as they maintain the
## respective credits only in the Python scripts, any information in the visual
## interface (GUI) can be modified without any implication.
##
## There are limitations on Qt licenses if you want to use your products
## commercially, I recommend reading them on the official website:
## https://doc.qt.io/qtforpython/licenses.html
##
################################################################################
import PySide2
import os

from PyQt5.QtCore import QSize
from PyQt5.QtWidgets import QMainWindow, QApplication
from pymitter import EventEmitter

from docker_manage import DockerEnv

dirname = os.path.dirname(PySide2.__file__)
plugin_path = os.path.join(dirname, 'plugins', 'platforms')
os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = plugin_path
import sys
import platform
# GUI FILE
from app_modules import *
from PyQt5 import QtCore
from PyQt5 import QtGui
from PyQt5 import QtWidgets
import PyQt5
import json
from glob import glob
from visualizer import make_graph

DESCRIPTIVE_STATISTICS_FOLDER = "..\\docker-hadoop\\map_reducers"


class MainWindow(QMainWindow):
    def clear_buttons(self, but_group):
        [but_group.removeButton(i) for i in but_group.buttons()]
    def delete_layout(self,layout):
        for i in reversed(range(layout.count())):
            layout.itemAt(i).widget().deleteLater()
    def clear_all_buttons(self):
        self.clear_buttons(self.ui.key_column_button_group)
        self.clear_buttons(self.ui.value_column_button_group)
        self.clear_buttons(self.ui.key_column_2_button_group)
        self.clear_buttons(self.ui.value_column_2_button_group)
        self.delete_layout(self.ui.KeyColumn_VerticalLayout)
        self.delete_layout(self.ui.ValueColumn2_VerticalLayout)
        self.delete_layout(self.ui.ValueColumn_VerticalLayout)
        self.delete_layout(self.ui.KeyColumn2_VerticalLayout)


    def createRadioButton(self):
        radioButton = QtWidgets.QRadioButton(self.ui.scrollAreaWidgetContents_19)
        font = radioButton.font()
        font.setPointSize(12)
        radioButton.setFont(font)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Preferred, QtWidgets.QSizePolicy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(radioButton.sizePolicy().hasHeightForWidth())
        radioButton.setSizePolicy(sizePolicy)
        radioButton.setMaximumSize(QtCore.QSize(16777215, 16777215))
        radioButton.setStyleSheet("QRadioButton::indicator:checked {\n"
                                  "    background-color: rgb(85, 255, 255);\n"
                                  "}")

        return radioButton

    def getInd(self, column_name):
        if column_name == "COUNT": return "count"
        return self.headers.index(column_name)

    def run_hadoop_func(self):
        try:
            parameters = {}
            parameters["key"] = self.getInd(self.ui.key_column_button_group.checkedButton().text())
            parameters["value"] = self.getInd(self.ui.value_column_button_group.checkedButton().text())
            parameters["function"] = self.ui.ds_button_group.checkedButton().text()
            parameters["dataset_path"] = self.ds_path
            if parameters["function"] == "AVERAGEIF":
                parameters["extra_options"] = {}
                parameters["extra_options"]["key2"] = self.getInd(
                    self.ui.key_column_2_button_group.checkedButton().text())
                parameters["extra_options"]["value2"] = self.getInd(
                    self.ui.value_column_2_button_group.checkedButton().text())
                parameters["extra_options"]["operator"] = \
                    self.ui.operator_button_group.checkedButton().text().split("(")[0]
            elif parameters["function"] == "LARGE":
                parameters["extra_options"] = {}
                parameters["extra_options"]["k"] = self.ui.k_number_input_field.text()
                # k sayisi numerik mi kontrolu yapilabilir belki
            print("DEBUG: Running hadoop operator Parameters:", parameters)

            response = self.docker.run(self.ds_path, parameters)
            self.docker.event_emitter.emit("OnStatusChange",
                                           "Finished running job function {}...".format(parameters["function"]))

            print("Response: ", response)
            graph_paths = make_graph(response)
            self.event_emitter.emit("OnMakeGraphsFinished", graph_paths)
        except Exception as e:
            print("ERROR: ", e)
            self.docker.status = str(e)

    def openDataset(self):
        try:
            self.ds_path = QFileDialog.getOpenFileName(self, 'Open file',
                                                       '')[0]

            self.ui.dataset_path.setText(self.ds_path)
            with open(self.ds_path, "r", encoding="utf-8") as fp:
                line = fp.readline().replace("\n", "")
                print("DEBUG: First Line:", line)
                delimeter = ","
                self.headers = line.split(delimeter)
            print("DEBUG: Headers:", self.headers)
            # BUTTON EKLENECEK
            self.clear_all_buttons()
            _translate = QtCore.QCoreApplication.translate
            for idx, header in enumerate(self.headers):
                if header == "":
                    header = "Unnamed Header"
                radioButton = self.createRadioButton()
                radioButton.setObjectName("radioButton_key" + str(idx))
                radioButton.setText(_translate("MainWindow", header))
                self.ui.key_column_button_group.addButton(radioButton)  # Key  group
                self.ui.KeyColumn_VerticalLayout.addWidget(radioButton)  # Key group

                radioButton = self.createRadioButton()
                radioButton.setObjectName("radioButton_key" + str(idx))
                radioButton.setText(_translate("MainWindow", header))
                self.ui.key_column_2_button_group.addButton(radioButton)  # second Key  group
                self.ui.KeyColumn2_VerticalLayout.addWidget(radioButton)  # second Key group

                radioButton = self.createRadioButton()
                radioButton.setObjectName("radioButton_value" + str(idx))
                radioButton.setText(_translate("MainWindow", header))
                self.ui.value_column_button_group.addButton(radioButton)  # Value  group
                self.ui.ValueColumn_VerticalLayout.addWidget(radioButton)  # Value  group

                radioButton = self.createRadioButton()
                radioButton.setObjectName("radioButton_value" + str(idx))
                radioButton.setText(_translate("MainWindow", header))
                self.ui.value_column_2_button_group.addButton(radioButton)  # second Value  group
                self.ui.ValueColumn2_VerticalLayout.addWidget(radioButton)  # second Value  group

            radioButton = self.createRadioButton()
            radioButton.setObjectName("radioButton_value" + str(idx))
            radioButton.setText(_translate("MainWindow", "COUNT"))
            self.ui.value_column_button_group.addButton(radioButton)  # Value  group
            self.ui.ValueColumn_VerticalLayout.addWidget(radioButton)  # Value  group

            # default secili gelen radio butonlar
            self.ui.key_column_button_group.buttons()[0].setChecked(True)
            self.ui.key_column_2_button_group.buttons()[0].setChecked(True)
            self.ui.value_column_2_button_group.buttons()[0].setChecked(True)
            self.ui.value_column_button_group.buttons()[0].setChecked(True)
            self.ui.operator_button_group.buttons()[0].setChecked(True)

            self.set_all_visible(True)
        except Exception as e:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Critical)
            msg.setText("Error")
            msg.setInformativeText(str(e))
            msg.setWindowTitle("An Error Occured!")
            msg.exec_()
        print("DEBUG : File selected:", self.ds_path)

    def set_all_visible(self, visible):
        self.ui.Frame_ValueColumn.setVisible(visible)
        self.ui.Frame_KeyColumn.setVisible(visible)
        self.ui.Frame_Function.setVisible(visible)
        self.ui.frame_4.setVisible(visible)
        self.ui.frame_3.setVisible(visible)
        self.ui.run_hadoop.setVisible(visible)
        self.ui.progressBar.setVisible(visible)

    def add_descriptive_statistics(self):
        _translate = QtCore.QCoreApplication.translate
        for folder in glob(os.path.join(DESCRIPTIVE_STATISTICS_FOLDER, "DS_*")):
            print(folder)
            if os.path.isdir(folder):
                radioButton = self.createRadioButton()
                name = os.path.basename(folder).replace("DS_", "").upper()

                radioButton.setObjectName(os.path.basename(folder))
                radioButton.setText(_translate("MainWindow", name))
                self.ui.ds_button_group.addButton(radioButton)
                self.ui.DS_VerticalLayout.addWidget(radioButton)

    def __init__(self):
        QMainWindow.__init__(self)
        self.ui = Ui_MainWindow()
        self.event_emitter = EventEmitter()
        self.event_emitter.on("OnMakeGraphsFinished", self.ui.show_graphs)
        self.ui.setupUi(self)
        self.add_descriptive_statistics()

        self.ui.select_file.clicked.connect(self.openDataset)
        self.ui.run_hadoop.clicked.connect(self.run_hadoop_func)

        ## PRINT ==> SYSTEM
        print('System: ' + platform.system())
        print('Version: ' + platform.release())

        ########################################################################
        ## START - WINDOW ATTRIBUTES
        ########################################################################

        ## REMOVE ==> STANDARD TITLE BAR
        UIFunctions.removeTitleBar(True)
        ## ==> END ##

        ## SET ==> WINDOW TITLE
        self.setWindowTitle('Main Window - Python Base')
        UIFunctions.labelTitle(self, 'Main Window - Python Base')
        # UIFunctions.labelDescription(self, 'Set text')
        ## ==> END ##

        ## WINDOW SIZE ==> DEFAULT SIZE
        startSize = QSize(1500, 1000)
        self.resize(startSize)
        self.setMinimumSize(QSize(500, 500))
        # UIFunctions.enableMaximumSize(self, 500, 720)
        ## ==> END ##

        ## ==> CREATE MENUS
        ########################################################################

        ## ==> TOGGLE MENU SIZE
        self.ui.btn_toggle_menu.clicked.connect(lambda: UIFunctions.toggleMenu(self, 220, True))
        ## ==> END ##

        ## ==> ADD CUSTOM MENUS
        self.ui.stackedWidget.setMinimumWidth(20)
        UIFunctions.addNewMenu(self, "HOME", "btn_home", "url(:/16x16/icons/16x16/cil-home.png)", True)
        # UIFunctions.addNewMenu(self, "Add User", "btn_new_user", "url(:/16x16/icons/16x16/cil-user-follow.png)", True)
        # UIFunctions.addNewMenu(self, "Custom Widgets", "btn_widgets", "url(:/16x16/icons/16x16/cil-equalizer.png),False)
        ## ==> END ##

        # START MENU => SELECTION
        UIFunctions.selectStandardMenu(self, "btn_home")
        ## ==> END ##

        ## ==> START PAGE
        self.ui.stackedWidget.setCurrentWidget(self.ui.page_home)

        ## ==> END ##

        ## USER ICON ==> SHOW HIDE
        # UIFunctions.userIcon(self, "WM", "", True)

        ## ==> END ##

        ## ==> MOVE WINDOW / MAXIMIZE / RESTORE
        ########################################################################
        def moveWindow(event):
            # IF MAXIMIZED CHANGE TO NORMAL
            if UIFunctions.returStatus() == 1:
                UIFunctions.maximize_restore(self)

            # MOVE WINDOW
            if event.buttons() == Qt.LeftButton:
                self.move(self.pos() + event.globalPos() - self.dragPos)
                self.dragPos = event.globalPos()
                event.accept()

        # WIDGET TO MOVE
        self.ui.frame_label_top_btns.mouseMoveEvent = moveWindow
        ## ==> END ##

        ## ==> LOAD DEFINITIONS
        ########################################################################
        UIFunctions.uiDefinitions(self)
        ## ==> END ##

        ########################################################################
        ## END - WINDOW ATTRIBUTES
        ############################## ---/--/--- ##############################

        ########################################################################
        #                                                                      #
        ## START -------------- WIDGETS FUNCTIONS/PARAMETERS ---------------- ##
        #                                                                      #
        ## ==> USER CODES BELLOW                                              ##
        ########################################################################

        ## ==> QTableWidget RARAMETERS
        ########################################################################
        # self.ui.tableWidget.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        ## ==> END ##

        ########################################################################
        #                                                                      #
        ## END --------------- WIDGETS FUNCTIONS/PARAMETERS ----------------- ##
        #                                                                      #
        ############################## ---/--/--- ##############################

        ## SHOW ==> MAIN WINDOW
        ########################################################################
        self.show()
        self.docker = DockerEnv()
        self.docker.event_emitter.on("OnMapReduceProgressChange", self.ui.update_progress_bar)
        self.docker.event_emitter.on("OnStatusChange", self.ui.update_status_label)
        self.docker.event_emitter.emit("OnStatusChange", "Ready")
        ## ==> END ##

    ########################################################################
    ## MENUS ==> DYNAMIC MENUS FUNCTIONS
    ########################################################################
    def Button(self):
        # GET BT CLICKED
        btnWidget = self.sender()

        # PAGE HOME
        if btnWidget.objectName() == "btn_home":
            self.ui.stackedWidget.setCurrentWidget(self.ui.page_home)
            UIFunctions.resetStyle(self, "btn_home")
            UIFunctions.labelPage(self, "Home")
            btnWidget.setStyleSheet(UIFunctions.selectMenu(btnWidget.styleSheet()))

        # PAGE NEW USER
        if btnWidget.objectName() == "btn_new_user":
            self.ui.stackedWidget.setCurrentWidget(self.ui.page_home)
            UIFunctions.resetStyle(self, "btn_new_user")
            UIFunctions.labelPage(self, "New User")
            btnWidget.setStyleSheet(UIFunctions.selectMenu(btnWidget.styleSheet()))

        # PAGE WIDGETS
        if btnWidget.objectName() == "btn_widgets":
            self.ui.stackedWidget.setCurrentWidget(self.ui.page_widgets)
            UIFunctions.resetStyle(self, "btn_widgets")
            UIFunctions.labelPage(self, "Custom Widgets")
            btnWidget.setStyleSheet(UIFunctions.selectMenu(btnWidget.styleSheet()))

    ## ==> END ##

    ########################################################################
    ## START ==> APP EVENTS
    ########################################################################

    ## EVENT ==> MOUSE DOUBLE CLICK
    ########################################################################
    def eventFilter(self, watched, event):
        if watched == self.le and event.type() == QtCore.QEvent.MouseButtonDblClick:
            print("pos: ", event.pos())

    ## ==> END ##

    ## EVENT ==> MOUSE CLICK
    ########################################################################
    def mousePressEvent(self, event):
        self.dragPos = event.globalPos()
        if event.buttons() == Qt.LeftButton:
            print('Mouse click: LEFT CLICK')
        if event.buttons() == Qt.RightButton:
            print('Mouse click: RIGHT CLICK')
        if event.buttons() == Qt.MidButton:
            print('Mouse click: MIDDLE BUTTON')

    ## ==> END ##

    ## EVENT ==> KEY PRESSED
    ########################################################################
    def keyPressEvent(self, event):
        print('Key: ' + str(event.key()) + ' | Text Press: ' + str(event.text()))

    ## ==> END ##

    ## EVENT ==> RESIZE EVENT
    ########################################################################
    def resizeEvent(self, event):
        self.resizeFunction()
        return super(MainWindow, self).resizeEvent(event)

    def resizeFunction(self):
        print('Height: ' + str(self.height()) + ' | Width: ' + str(self.width()))
    ## ==> END ##

    ########################################################################
    ## END ==> APP EVENTS
    ############################## ---/--/--- ##############################


if __name__ == "__main__":
    app = QApplication(sys.argv)
    QtGui.QFontDatabase.addApplicationFont('fonts/segoeui.ttf')
    QtGui.QFontDatabase.addApplicationFont('fonts/segoeuib.ttf')
    window = MainWindow()
    sys.exit(app.exec_())
