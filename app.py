import sys
import os
import time
import threading
import traceback
from datetime import datetime
import psutil

# --- Imports for PySide6 ---
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QPushButton, QLineEdit, QCheckBox, QFrame, QProgressBar, QScrollArea,
    QComboBox, QTreeWidget, QTreeWidgetItem, QTextEdit, QFileDialog, QMessageBox,
    QSizePolicy, QHeaderView
)
from PySide6.QtCore import Qt, QThread, Signal, QObject, Slot
from PySide6.QtGui import QFont, QColor, QPalette

# --- Import theme library ---
from qt_material import apply_stylesheet

# --- Import from our existing modules ---
import config
import file_operations
import drive_manager
from kafka_producer import KafkaManager

def get_drive_name_from_mountpoint(mountpoint):
    """Gets a clean, usable folder name from a drive's mountpoint."""
    if not mountpoint:
        return "Unknown_Drive"
    if sys.platform == "win32" and ":" in mountpoint:
        return mountpoint.split(":")[0]
    name = os.path.basename(mountpoint)
    return name if name else "Unknown_Drive"

# --- Worker for background tasks ---

class WorkerSignals(QObject):
    """Defines the signals available from a running worker thread."""
    finished = Signal(object, str, list)  # results, mountpoint, successfully_copied_paths
    progress = Signal(int, int, str)      # current_fil_num, total_files, filename
    file_status = Signal(str, str, str, object, object) # item_id, status_text, tag, progress_value, speed_mbps
    file_time = Signal(str, float)        # item_id, duration
    log = Signal(str, str)                # message, level

class CopyWorker(QObject):
    """Handles the copy process in a separate thread."""
    def __init__(self, videos, destination_root, should_wipe, item_ids, process_id, conflict_policy):
        super().__init__()
        self.videos = videos
        self.destination_root = destination_root
        self.should_wipe = should_wipe
        self.item_ids = item_ids
        self.process_id = process_id
        self.conflict_policy = conflict_policy
        self.signals = WorkerSignals()
        self.is_running = True

    def run(self):
        try:
            final_results = {"success": 0, "error": 0, "skipped": 0}
            successfully_copied_paths = []
            total_files = len(self.videos)

            for i, video_info in enumerate(self.videos):
                if not self.is_running:
                    break
                
                start_time = time.time()
                source_path = video_info['path']
                file_name = os.path.basename(source_path)
                item_id = self.item_ids[i]

                self.signals.progress.emit(i + 1, total_files, file_name)

                drive_name = get_drive_name_from_mountpoint(self.process_id)
                final_destination_folder = os.path.join(self.destination_root, drive_name)
                try:
                    os.makedirs(final_destination_folder, exist_ok=True)
                except OSError as e:
                    self.signals.log.emit(f"Không thể tạo thư mục {final_destination_folder}: {e}", "ERROR")
                    final_results["error"] += 1
                    continue

                def status_callback(status_key, status_text, progress_value=None, speed_mbps=None):
                    self.signals.file_status.emit(item_id, status_text, status_key, progress_value, speed_mbps)
                    if "Sao chép (" not in status_text:
                        self.signals.log.emit(f"{file_name}: {status_text}", status_key.upper())

                try:
                    success, skipped, final_dest_path = file_operations.copy_and_verify_file(
                        source_path, final_destination_folder, self.conflict_policy, status_callback
                    )
                    if success:
                        final_results["success"] += 1
                        if skipped:
                            final_results["skipped"] += 1
                        else:
                            if final_dest_path:
                                successfully_copied_paths.append(final_dest_path)
                    else:
                        final_results["error"] += 1
                except Exception as e:
                    final_results["error"] += 1
                    status_callback("error", f"Lỗi nghiêm trọng: {e}", -1.0)

                duration = time.time() - start_time
                self.signals.file_time.emit(item_id, duration)

            # --- Finalize ---
            if self.is_running:
                all_files_processed_successfully = final_results["error"] == 0
                drive_name_for_report = get_drive_name_from_mountpoint(self.process_id)

                if all_files_processed_successfully:
                    if self.should_wipe:
                        def wipe_status_callback(status_key, status_text, progress_value=None):
                            self.signals.log.emit(f"Xóa thẻ {drive_name_for_report}: {status_text}", status_key.upper())
                        
                        self.signals.log.emit(f"Bắt đầu xóa sạch thẻ {drive_name_for_report}...", "INFO")
                        wipe_success, wipe_message = file_operations.wipe_drive_data(self.process_id, wipe_status_callback)
                        self.signals.log.emit(f"Kết quả xóa thẻ {drive_name_for_report}: {wipe_message}", "SUCCESS" if wipe_success else "ERROR")

                    # Eject logic will be handled in the main thread after this worker finishes
            
            self.signals.finished.emit(final_results, self.process_id, successfully_copied_paths)

        except Exception:
            tb_str = traceback.format_exc()
            self.signals.log.emit(f"Lỗi nghiêm trọng trong luồng sao chép: {tb_str}", "CRITICAL")
            self.signals.finished.emit({"success": 0, "error": len(self.videos), "skipped": 0}, self.process_id, [])

    def stop(self):
        self.is_running = False

class DriveMonitor(QObject):
    """Monitors drive changes in a background thread."""
    drives_changed = Signal()
    log = Signal(str, str)
    
    def __init__(self):
        super().__init__()
        self.monitoring = True

    def run(self):
        known_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
        while self.monitoring:
            try:
                if sys.platform.startswith("linux"):
                    drive_manager.find_and_mount_unmounted_drives()

                current_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
                if current_mountpoints != known_mountpoints:
                    self.drives_changed.emit()
                    known_mountpoints = current_mountpoints
            except Exception as e:
                self.log.emit(f"Lỗi trong luồng giám sát ổ đĩa: {e}", "ERROR")
            time.sleep(3)

    def stop(self):
        self.monitoring = False

# --- Custom Widgets ---

class DriveWidget(QWidget):
    selection_changed = Signal(str, bool)

    def __init__(self, mountpoint, description):
        super().__init__()
        self.mountpoint = mountpoint
        
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 5)
        main_layout.setSpacing(2)

        container_frame = QFrame(self)
        container_frame.setObjectName("DriveWidgetFrame")
        container_frame.setProperty("class", "card")
        
        frame_layout = QGridLayout(container_frame)
        frame_layout.setContentsMargins(10, 5, 10, 5)

        self.checkbox = QCheckBox()
        self.checkbox.stateChanged.connect(self.on_toggle)
        frame_layout.addWidget(self.checkbox, 0, 0, 2, 1)

        self.name_label = QLabel(get_drive_name_from_mountpoint(mountpoint))
        self.name_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        frame_layout.addWidget(self.name_label, 0, 1)
        
        self.speed_label = QLabel("")
        self.speed_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        frame_layout.addWidget(self.speed_label, 0, 2)
        
        self.description_label = QLabel(description)
        palette = self.description_label.palette()
        palette.setColor(QPalette.ColorRole.WindowText, QColor("gray"))
        self.description_label.setPalette(palette)
        frame_layout.addWidget(self.description_label, 1, 1, 1, 2)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(5)
        self.progress_bar.setValue(0)
        
        main_layout.addWidget(container_frame)
        main_layout.addWidget(self.progress_bar)
        
        self.setStyleSheet("""
            QFrame#DriveWidgetFrame {
                background-color: #2E2E2E;
                border-radius: 10px;
            }
        """)

    def on_toggle(self, state):
        self.selection_changed.emit(self.mountpoint, state == Qt.CheckState.Checked.value)

    def start_scan(self):
        self.progress_bar.setRange(0, 0) # Indeterminate mode
        self.checkbox.setEnabled(False)

    def finish_scan(self):
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100)
        self.checkbox.setEnabled(True)

    def reset(self):
        self.checkbox.setChecked(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.clear_speed()
    
    def show_ejected_status(self):
        self.checkbox.setEnabled(False)
        self.description_label.setText("✔ Đã tháo an toàn")
        palette = self.description_label.palette()
        palette.setColor(QPalette.ColorRole.WindowText, QColor(config.COLOR_STATUS_SUCCESS))
        self.description_label.setPalette(palette)
        self.progress_bar.setValue(100)
        self.clear_speed()

    def update_speed(self, speed_mbps):
        self.speed_label.setText(f"{speed_mbps:.1f} MB/s")

    def clear_speed(self):
        self.speed_label.setText("")

# --- Main Application Window ---

class AutoCopierApp(QMainWindow):
    def __init__(self):
        super().__init__()
        
        # --- Window Setup ---
        self.setWindowTitle("Auto Video Copier Pro+")
        self.setGeometry(100, 100, 1100, 900)

        # --- App State Variables ---
        self.destination_path = ""
        self.delete_after_copy = False
        self.is_auto_mode = False
        self.file_extensions = ""
        self.conflict_policy = ""
        self.kafka_servers = ""
        self.kafka_topic = ""
        
        self.detected_drives = {}
        self.video_item_map = {}
        self.selected_drives = set()
        self.drive_widgets = {}
        self.active_copy_processes = 0
        self.active_speeds = {}
        self.batch_results = []
        self.kafka_manager = KafkaManager(app_logger_callback=self.log_message)
        self.copy_threads = {}

        # --- Load and Build ---
        self.load_app_config()
        self.create_widgets()
        self.setup_treeview()
        
        self.kafka_manager.configure_producer(self.kafka_servers)
        
        # --- Start Background Processes ---
        self.start_drive_monitor()
        self.update_drive_list()

    def _update_ui_states(self):
        is_copying = self.active_copy_processes > 0

        self.mode_switch.setEnabled(not is_copying)

        if self.is_auto_mode:
            self.delete_checkbox.setChecked(True)
            self.delete_checkbox.setEnabled(False)
        else:
            self.delete_checkbox.setEnabled(not is_copying)
        
        for widget in self.drive_widgets.values():
            widget.checkbox.setEnabled(not self.is_auto_mode and not is_copying)

        has_files_in_list = self.video_tree.topLevelItemCount() > 0
        can_select = has_files_in_list and not self.is_auto_mode and not is_copying
        self.select_all_button.setEnabled(can_select)
        self.deselect_all_button.setEnabled(can_select)

        has_selection = len(self.video_tree.selectedItems()) > 0
        can_copy = has_selection and not self.is_auto_mode and not is_copying
        self.copy_button.setEnabled(can_copy)

    def closeEvent(self, event):
        """Handle window close event."""
        self.log_message("Đang đóng ứng dụng...", "INFO")
        # Stop all running copy threads first
        for thread, worker in self.copy_threads.values():
            if thread.isRunning():
                worker.stop()
                thread.quit()
                thread.wait(2000) # Wait up to 2s for thread to finish

        # Then stop the drive monitor thread
        if self.monitor_thread and self.monitor_thread.isRunning():
            self.drive_monitor.stop()
            self.monitor_thread.quit()
            self.monitor_thread.wait(5000) # Wait up to 5s for the thread to stop
        event.accept()

    def load_app_config(self):
        conf = config.load_config()
        self.destination_path = conf.get("destination_path", "")
        self.file_extensions = conf.get("video_extensions", config.DEFAULT_VIDEO_EXTENSIONS)
        self.conflict_policy = conf.get("conflict_policy", config.DEFAULT_CONFLICT_POLICY)
        self.kafka_servers = conf.get("kafka_servers", config.DEFAULT_KAFKA_SERVERS)
        self.kafka_topic = conf.get("kafka_topic", config.DEFAULT_KAFKA_TOPIC)

    def save_app_config(self):
        conf_data = {
            "destination_path": self.destination_path,
            "video_extensions": self.ext_entry.text(),
            "conflict_policy": self.conflict_menu.currentText()
        }
        config.save_config(conf_data)

    def browse_destination(self):
        folder_selected = QFileDialog.getExistingDirectory(self, "Chọn Thư Mục Đích")
        if folder_selected:
            self.destination_path = folder_selected
            self.dest_entry.setText(self.destination_path)
            self.save_app_config()

    # --- UI Creation ---

    def create_widgets(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        top_frame = QFrame()
        top_layout = QHBoxLayout(top_frame)
        
        self._create_left_panel(top_layout)
        self._create_top_right_controls(top_layout)

        bottom_frame = QFrame()
        bottom_layout = QHBoxLayout(bottom_frame)
        self._create_file_list_panel(bottom_layout)
        self._create_log_panel(bottom_layout)
        
        main_layout.addWidget(top_frame)
        main_layout.addWidget(bottom_frame, 1) # Make bottom frame expand

        self._create_progress_frame(main_layout)
        self._create_action_frame(main_layout)

    def _create_left_panel(self, master_layout):
        left_panel = QFrame()
        left_panel.setFixedWidth(350)
        left_panel.setProperty("class", "card")
        panel_layout = QVBoxLayout(left_panel)
        
        panel_layout.addWidget(QLabel("Ổ Đĩa / Thẻ Nhớ", font=QFont("Arial", 12, QFont.Weight.Bold)))
        
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        
        self.drive_list_widget = QWidget()
        self.drive_list_layout = QVBoxLayout(self.drive_list_widget)
        self.drive_list_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        scroll_area.setWidget(self.drive_list_widget)
        
        panel_layout.addWidget(scroll_area)

        refresh_button = QPushButton("Làm Mới")
        refresh_button.clicked.connect(self.update_drive_list)
        panel_layout.addWidget(refresh_button)
        
        master_layout.addWidget(left_panel)

    def _create_top_right_controls(self, master_layout):
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)

        # Destination Frame
        dest_frame = QFrame()
        dest_frame.setProperty("class", "card")
        dest_layout = QGridLayout(dest_frame)
        dest_layout.addWidget(QLabel("Thư Mục Đích:", font=QFont("Arial", 10, QFont.Weight.Bold)), 0, 0, 1, 3)
        self.dest_entry = QLineEdit(self.destination_path)
        self.dest_entry.setReadOnly(True)
        dest_layout.addWidget(self.dest_entry, 1, 0, 1, 2)
        browse_button = QPushButton("...")
        browse_button.setFixedWidth(40)
        browse_button.clicked.connect(self.browse_destination)
        dest_layout.addWidget(browse_button, 1, 2)
        
        # Mode & Settings
        mode_settings_layout = QHBoxLayout()
        
        mode_frame = QFrame()
        mode_frame.setProperty("class", "card")
        mode_layout = QVBoxLayout(mode_frame)
        mode_layout.addWidget(QLabel("Chế Độ", font=QFont("Arial", 10, QFont.Weight.Bold)))
        self.mode_switch = QCheckBox("Tự Động")
        self.mode_switch.stateChanged.connect(self.toggle_auto_mode)
        mode_layout.addWidget(self.mode_switch)
        
        settings_frame = QFrame()
        settings_frame.setProperty("class", "card")
        settings_layout = QGridLayout(settings_frame)
        settings_layout.addWidget(QLabel("Cài Đặt Nâng Cao", font=QFont("Arial", 10, QFont.Weight.Bold)), 0, 0, 1, 2)
        settings_layout.addWidget(QLabel("Các loại file:"), 1, 0)
        self.ext_entry = QLineEdit(self.file_extensions)
        self.ext_entry.textChanged.connect(self.save_app_config)
        settings_layout.addWidget(self.ext_entry, 1, 1)
        settings_layout.addWidget(QLabel("Khi file trùng:"), 2, 0)
        self.conflict_menu = QComboBox()
        self.conflict_menu.addItems(["Bỏ Qua", "Ghi Đè", "Đổi Tên"])
        self.conflict_menu.setCurrentText(self.conflict_policy)
        self.conflict_menu.currentTextChanged.connect(self.save_app_config)
        settings_layout.addWidget(self.conflict_menu, 2, 1)

        mode_settings_layout.addWidget(mode_frame)
        mode_settings_layout.addWidget(settings_frame, 1)

        right_layout.addWidget(dest_frame)
        right_layout.addLayout(mode_settings_layout)

        master_layout.addWidget(right_panel, 1)

    def _create_file_list_panel(self, master_layout):
        file_list_container = QFrame()
        file_list_container.setProperty("class", "card")
        layout = QVBoxLayout(file_list_container)
        layout.addWidget(QLabel("Danh Sách File", font=QFont("Arial", 12, QFont.Weight.Bold)))
        
        self.video_tree = QTreeWidget()
        layout.addWidget(self.video_tree)
        
        master_layout.addWidget(file_list_container, 1)

    def _create_log_panel(self, master_layout):
        log_container = QFrame()
        log_container.setProperty("class", "card")
        layout = QVBoxLayout(log_container)
        layout.addWidget(QLabel("Nhật Ký Hoạt Động", font=QFont("Arial", 12, QFont.Weight.Bold)))
        
        self.log_textbox = QTextEdit()
        self.log_textbox.setReadOnly(True)
        self.log_textbox.setFont(QFont("Courier New", 10))
        layout.addWidget(self.log_textbox)

        master_layout.addWidget(log_container, 1)

    def _create_progress_frame(self, master_layout):
        self.progress_frame = QFrame()
        self.progress_frame.setProperty("class", "card")
        layout = QGridLayout(self.progress_frame)
        self.progress_status_label = QLabel("Sẵn sàng")
        self.progress_speed_label = QLabel("")
        self.progress_speed_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setValue(0)
        
        layout.addWidget(self.progress_status_label, 0, 0)
        layout.addWidget(self.progress_speed_label, 0, 1)
        layout.addWidget(self.progress_bar, 1, 0, 1, 2)
        
        master_layout.addWidget(self.progress_frame)
        self.progress_frame.hide()

    def _create_action_frame(self, master_layout):
        action_frame = QFrame()
        action_frame.setProperty("class", "card")
        layout = QHBoxLayout(action_frame)
        
        self.select_all_button = QPushButton("Chọn Tất Cả")
        self.select_all_button.clicked.connect(self.select_all_videos)
        self.deselect_all_button = QPushButton("Bỏ Chọn Tất Cả")
        self.deselect_all_button.clicked.connect(self.deselect_all_videos)
        
        self.delete_checkbox = QCheckBox("Xóa sạch thẻ sau khi chép")
        self.delete_checkbox.stateChanged.connect(lambda state: setattr(self, 'delete_after_copy', state == Qt.CheckState.Checked.value))
        
        self.copy_button = QPushButton("Sao Chép Thủ Công")
        self.copy_button.setObjectName("CopyButton")
        self.copy_button.clicked.connect(self.start_manual_copy)

        layout.addWidget(self.select_all_button)
        layout.addWidget(self.deselect_all_button)
        layout.addSpacing(20)
        layout.addWidget(self.delete_checkbox)
        layout.addStretch(1)
        layout.addWidget(self.copy_button)
        
        master_layout.addWidget(action_frame)

    def setup_treeview(self):
        headers = ["Trạng Thái", "Tên Tệp", "Kích Thước", "Thiết Bị", "Thời Gian", "Tiến Trình"]
        self.video_tree.setColumnCount(len(headers))
        self.video_tree.setHeaderLabels(headers)
        self.video_tree.header().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.video_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch) # Name column
        self.video_tree.setSelectionMode(QTreeWidget.SelectionMode.ExtendedSelection)

    def log_message(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] [{level}]: {message}"
        self.log_textbox.append(formatted_message)

    # --- Core Logic and Event Handlers ---

    def start_drive_monitor(self):
        self.monitor_thread = QThread()
        self.drive_monitor = DriveMonitor()
        self.drive_monitor.moveToThread(self.monitor_thread)
        self.drive_monitor.drives_changed.connect(self.on_drives_changed)
        self.drive_monitor.log.connect(self.log_message)
        self.monitor_thread.started.connect(self.drive_monitor.run)
        self.monitor_thread.start()

    def on_drives_changed(self):
        # First, determine which drives are new *before* updating the UI
        current_mountpoints = {d.mountpoint for d in drive_manager.get_removable_drives()}
        existing_mountpoints = set(self.drive_widgets.keys())
        new_drives = current_mountpoints - existing_mountpoints

        # Second, update the UI to reflect the current state
        self.update_drive_list()

        # Third, if in auto mode, process the new drives we found earlier
        if self.is_auto_mode:
            for mountpoint in new_drives:
                 self.log_message(f"Tự động: Phát hiện thẻ mới {mountpoint}. Bắt đầu xử lý.", "INFO")
                 self.start_auto_process(mountpoint)


    def toggle_auto_mode(self, state):
        self.is_auto_mode = (state == Qt.CheckState.Checked.value)
        if self.is_auto_mode:
            self.log_message("Chế độ TỰ ĐỘNG đã được BẬT.", "INFO")
            for widget in self.drive_widgets.values():
                if widget.checkbox.isChecked():
                    widget.reset()
            self.selected_drives.clear()
            self.clear_video_list()
            self.log_message(f"Tự động: Bắt đầu xử lý {len(self.drive_widgets)} thẻ hiện có...", "INFO")
            for mountpoint in self.drive_widgets.keys():
                self.start_auto_process(mountpoint)
        else:
            self.log_message("Chế độ TỰ ĐỘNG đã được TẮT.", "INFO")
        self._update_ui_states()

    def update_drive_list(self):
        current_drives = drive_manager.get_removable_drives()
        current_mountpoints = {d.mountpoint for d in current_drives}
        existing_mountpoints = set(self.drive_widgets.keys())

        for mountpoint in existing_mountpoints - current_mountpoints:
            widget = self.drive_widgets.pop(mountpoint)
            widget.deleteLater()
            if mountpoint in self.selected_drives:
                self.selected_drives.remove(mountpoint)
                self.clear_video_list_for_drive(mountpoint)

        for drive in current_drives:
            if drive.mountpoint not in self.drive_widgets:
                drive_label = get_drive_name_from_mountpoint(drive.mountpoint)
                description = f"{drive_label} ({drive.fstype})" if drive_label and drive_label != "/" else f"{drive.device} ({drive.fstype})"
                widget = DriveWidget(drive.mountpoint, description)
                widget.selection_changed.connect(self.on_drive_selection_changed)
                self.drive_list_layout.addWidget(widget)
                self.drive_widgets[drive.mountpoint] = widget

        self.detected_drives = {d.mountpoint: {'device': d.device, 'description': d.opts} for d in current_drives}

    def on_drive_selection_changed(self, mountpoint, is_selected):
        if self.is_auto_mode: return
        if is_selected:
            if mountpoint not in self.selected_drives:
                self.selected_drives.add(mountpoint)
                widget = self.drive_widgets.get(mountpoint)
                if widget:
                    widget.start_scan()
                threading.Thread(target=self.list_videos_from_drive, args=(mountpoint,), daemon=True).start()
        else:
            if mountpoint in self.selected_drives:
                self.selected_drives.remove(mountpoint)
                self.clear_video_list_for_drive(mountpoint)
        self._update_ui_states()

    def list_videos_from_drive(self, drive_path):
        widget = self.drive_widgets.get(drive_path)
        if not widget: return

        self.log_message(f"Đang tìm kiếm file trên {get_drive_name_from_mountpoint(drive_path)}...", "INFO")
        if drive_path not in self.detected_drives:
            self.log_message(f"Hủy quét: {get_drive_name_from_mountpoint(drive_path)} không còn được kết nối.", "WARN")
            if widget: widget.finish_scan()
            return
        
        try:
            extensions = self.ext_entry.text()
            found_videos = list(file_operations.find_files_on_drive(drive_path, extensions))
            QApplication.instance().postEvent(self, CustomEvent("populate_list", drive_path=drive_path, videos=found_videos))
        except Exception as e:
            self.log_message(f"Lỗi khi quét file trên {drive_path}: {e}", "ERROR")

    def customEvent(self, event):
        if event.type() == CustomEvent.type():
            if event.event_id == "populate_list":
                self._populate_list_after_scan(event.drive_path, event.videos)
            elif event.event_id == "start_auto_copy":
                self._populate_and_start_auto_copy(event.mountpoint, event.videos)

    def _populate_list_after_scan(self, drive_path, found_videos):
        if drive_path in self.drive_widgets:
            self.drive_widgets[drive_path].finish_scan()
        
        if not found_videos:
            self.log_message(f"Không tìm thấy file video nào phù hợp trên {get_drive_name_from_mountpoint(drive_path)}.", "WARN")
        else:
            new_items = []
            for video_data in found_videos:
                item = self.add_video_to_list(video_data)
                if item: new_items.append(item)
            
            for item in new_items:
                item.setSelected(True)
        self._update_ui_states()

    def add_video_to_list(self, video_data):
        try:
            size_mb = video_data['size'] / (1024*1024)
            drive_name = get_drive_name_from_mountpoint(video_data.get('drive', 'N/A'))
            
            item = QTreeWidgetItem([
                "Sẵn sàng",
                os.path.basename(video_data['path']),
                f"{size_mb:.2f} MB",
                drive_name,
                "", # Time
                ""  # Progress
            ])
            
            item_id = id(item)
            self.video_tree.addTopLevelItem(item)
            self.video_item_map[str(item_id)] = { "item": item, "path": video_data['path'], "drive": video_data['drive'] }
            return item
        except Exception as e:
            print(f"Lỗi khi thêm video vào danh sách: {e}")
            return None

    def clear_video_list_for_drive(self, mountpoint_to_clear):
        drive_name_to_clear = get_drive_name_from_mountpoint(mountpoint_to_clear)
        items_to_remove = []
        for item_id_str, data in list(self.video_item_map.items()):
            if data['drive'] == mountpoint_to_clear:
                items_to_remove.append((item_id_str, data['item']))
        
        for item_id_str, item in items_to_remove:
            root = self.video_tree.invisibleRootItem()
            (item.parent() or root).removeChild(item)
            del self.video_item_map[item_id_str]

        self._update_ui_states()
        self.log_message(f"Đã xóa file từ {drive_name_to_clear} khỏi danh sách.", "INFO")

    def select_all_videos(self):
        self.video_tree.selectAll()
        self._update_ui_states()

    def deselect_all_videos(self):
        self.video_tree.clearSelection()
        self._update_ui_states()

    def start_manual_copy(self):
        selected_items = self.video_tree.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Chưa chọn file", "Vui lòng chọn ít nhất một file để sao chép.")
            return
        
        if not self.destination_path:
            QMessageBox.warning(self, "Chưa chọn đích", "Vui lòng chọn thư mục đích trước khi sao chép.")
            return

        videos_by_drive = {}
        for item in selected_items:
            item_id = str(id(item))
            if item_id in self.video_item_map:
                video_info = self.video_item_map[item_id]
            mountpoint = video_info['drive']
            if mountpoint not in videos_by_drive:
                videos_by_drive[mountpoint] = []
                videos_by_drive[mountpoint].append((video_info, item))

        if self.delete_after_copy:
            drive_names = ", ".join([get_drive_name_from_mountpoint(mp) for mp in videos_by_drive.keys()])
            reply = QMessageBox.question(self, "Xác Nhận Xóa Sạch Thẻ",
                                         f"Bạn có chắc chắn muốn XÓA VĨNH VIỄN TOÀN BỘ DỮ LIỆU trên các thẻ ({drive_names}) sau khi sao chép thành công không?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.No:
                return

        for mountpoint, video_item_pairs in videos_by_drive.items():
            videos_to_process = [pair[0] for pair in video_item_pairs]
            item_ids = [str(id(pair[1])) for pair in video_item_pairs]
            self.start_copy_process(mountpoint, videos_to_process, item_ids)

    def start_auto_process(self, mountpoint):
        if not self.is_auto_mode: return
        widget = self.drive_widgets.get(mountpoint)
        if widget:
            widget.start_scan()
        
        # This is a short-lived thread just for scanning
        threading.Thread(target=self._auto_process_thread, args=(mountpoint,), daemon=True).start()

    def _auto_process_thread(self, mountpoint):
        """Worker thread for auto-scanning."""
        try:
            self.log_message(f"Tự động: Đang quét {mountpoint}...", "INFO")
            extensions = self.ext_entry.text()
            videos_to_process = list(file_operations.find_files_on_drive(mountpoint, extensions))
            QApplication.instance().postEvent(self, CustomEvent("start_auto_copy", mountpoint=mountpoint, videos=videos_to_process))
        except Exception as e:
            self.log_message(f"Lỗi khi tự động quét {mountpoint}: {e}", "ERROR")

    def _populate_and_start_auto_copy(self, mountpoint, videos_to_process):
        widget = self.drive_widgets.get(mountpoint)
        if widget: widget.finish_scan()

        if not videos_to_process:
            self.log_message(f"Tự động: Không tìm thấy file phù hợp trên {mountpoint}.", "INFO")
            self._start_eject_drive(mountpoint)
            return

        item_ids = [str(id(self.add_video_to_list(video_data))) for video_data in videos_to_process]
        
        self.log_message(f"Tự động: Tìm thấy {len(videos_to_process)} file. Bắt đầu sao chép.", "INFO")
        self.start_copy_process(mountpoint, videos_to_process, item_ids)

    def start_copy_process(self, mountpoint, videos_to_process, item_ids):
        if not self.destination_path or not os.path.isdir(self.destination_path):
            QMessageBox.critical(self, "Lỗi", "Vui lòng chọn một thư mục đích hợp lệ.")
            return
        
        # Disk space check
        required_space = file_operations.get_required_space([v['path'] for v in videos_to_process])
        if not file_operations.has_enough_space(self.destination_path, required_space):
            QMessageBox.critical(self, "Thiếu Dung Lượng", "Không đủ dung lượng trống tại thư mục đích.")
            return

        self.progress_frame.show()
        self.progress_bar.setValue(0)
        self.progress_status_label.setText("Chuẩn bị sao chép...")

        self.active_copy_processes += 1
        self._update_ui_states()

        thread = QThread()
        worker = CopyWorker(videos_to_process, self.destination_path, self.delete_after_copy, item_ids, mountpoint, self.conflict_menu.currentText())
        worker.moveToThread(thread)
        
        worker.signals.finished.connect(self._finalize_copy_process)
        worker.signals.progress.connect(self.update_overall_progress)
        worker.signals.file_status.connect(self.update_item_status)
        worker.signals.file_time.connect(self.update_item_time)
        worker.signals.log.connect(self.log_message)
        
        thread.started.connect(worker.run)
        thread.start()

        self.copy_threads[mountpoint] = (thread, worker)

    @Slot(int, int, str)
    def update_overall_progress(self, current, total, filename):
        progress_value = int((current / total) * 100)
        self.progress_bar.setValue(progress_value)
        self.progress_status_label.setText(f"Đang xử lý {current}/{total}: {filename}")

    @Slot(str, str, str, object, object)
    def update_item_status(self, item_id, status_text, tag, progress_value, speed_mbps):
        if item_id in self.video_item_map:
            item = self.video_item_map[item_id]["item"]
            
            # Update status text
            final_text = status_text
            if tag == 'success':
                final_text = f"✔ {status_text}"
            item.setText(0, final_text)

            # Update progress bar
            if progress_value is not None:
                if progress_value == -1.0:
                    progress_bar = "[!!! LỖI !!!]"
                else:
                    bar_length = 10
                    filled = int(bar_length * progress_value)
                    bar = '█' * filled + '-' * (bar_length - filled)
                    progress_bar = f"[{bar}] {int(progress_value * 100)}%"
                item.setText(5, progress_bar)

            # Update color based on tag
            color = QColor("white")
            if tag == 'processing':
                color = QColor(config.COLOR_STATUS_WARN)
            elif tag == 'success':
                color = QColor(config.COLOR_STATUS_SUCCESS)
            elif tag == 'error':
                color = QColor(config.COLOR_STATUS_ERROR)
            for i in range(item.columnCount()):
                item.setForeground(i, color)
            
    @Slot(str, float)
    def update_item_time(self, item_id, duration):
        if item_id in self.video_item_map:
            self.video_item_map[item_id]["item"].setText(4, f"{duration:.2f} s")
            
    def _finalize_copy_process(self, results, mountpoint, successfully_copied_paths):
        drive_name = get_drive_name_from_mountpoint(mountpoint)
        self.batch_results.append({'drive_name': drive_name, 'results': results})

        if mountpoint in self.drive_widgets:
            self.drive_widgets[mountpoint].clear_speed()
            if results['error'] == 0:
                self._start_eject_drive(mountpoint)

        # Cleanup thread
        if mountpoint in self.copy_threads:
            thread, worker = self.copy_threads.pop(mountpoint)
            thread.quit()
            thread.wait()

        self.active_copy_processes -= 1
        self.log_message(f"Hoàn tất quá trình cho {drive_name}.")

        if self.active_copy_processes == 0:
            self.progress_frame.hide()
            self.show_consolidated_report()
        
        if successfully_copied_paths:
            threading.Thread(target=self._send_kafka_message_thread, args=(successfully_copied_paths, drive_name), daemon=True).start()

        self._update_ui_states()
        
    def _start_eject_drive(self, mountpoint):
        drive_name = get_drive_name_from_mountpoint(mountpoint)
        self.log_message(f"Bắt đầu tự động tháo {drive_name}.", "INFO")
        if mountpoint not in self.detected_drives:
            self.log_message(f"Không thể tháo {drive_name}, thiết bị không còn được kết nối.", "WARN")
            return
        threading.Thread(target=self._eject_drive_thread, args=(mountpoint,), daemon=True).start()

    def _eject_drive_thread(self, mountpoint):
        drive_name = get_drive_name_from_mountpoint(mountpoint)
        self.log_message(f"Đang tháo {drive_name}...", "INFO")
        drive_info = self.detected_drives.get(mountpoint)
        if not drive_info:
            self.log_message(f"Lỗi khi tháo {drive_name}: Không tìm thấy thông tin thiết bị.", "ERROR")
            return

        success, message = drive_manager.eject_drive(drive_info['device'])
        
        if success:
            self.log_message(f"Đã tháo thành công {drive_name}.", "SUCCESS")
            widget = self.drive_widgets.get(mountpoint)
            if widget:
                # Need to update UI on main thread
                QApplication.instance().postEvent(widget, CustomEvent("ejected"))
            else:
                self.log_message(f"Lỗi khi tháo {drive_name}: {message}", "ERROR")

    def _send_kafka_message_thread(self, file_paths, drive_name):
        message = {
            'timestamp': datetime.now().isoformat(),
            'source_app': 'AutoCopierApp',
            'event_type': 'copy_complete',
            'drive_name': drive_name,
            'copied_files': file_paths,
            'file_count': len(file_paths)
        }
        self.kafka_manager.send_message(self.kafka_topic, message)

    def show_consolidated_report(self):
        if not self.batch_results: return
        # ... (Implementation is the same as the original, using QMessageBox)
        self.batch_results.clear()
        self.reset_for_next_session()

    def clear_video_list(self):
        self.video_tree.clear()
        self.video_item_map.clear()
        self._update_ui_states()

    def reset_for_next_session(self):
        self.log_message("Sẵn sàng cho phiên làm việc tiếp theo.", "INFO")
        self.clear_video_list()
        self.selected_drives.clear()
        for widget in self.drive_widgets.values():
            widget.reset()
        self._update_ui_states()

# Custom Event class for safe cross-thread UI updates
from PySide6.QtCore import QEvent

class CustomEvent(QEvent):
    _type = QEvent.Type(QEvent.registerEventType())

    def __init__(self, event_id, **data):
        super().__init__(self._type)
        self.event_id = event_id
        self.data = data
        for key, value in data.items():
            setattr(self, key, value)
    
    @staticmethod
    def type():
        return CustomEvent._type

# Monkey patch QWidget to handle our custom event
def customEvent_patch(self, event):
    if event.type() == CustomEvent.type():
        if event.event_id == "ejected":
            self.show_ejected_status()
        return True
    return QWidget.customEvent(self, event)

DriveWidget.customEvent = customEvent_patch

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Apply dark theme
    apply_stylesheet(app, theme='dark_teal.xml')
    
    # Custom styling
    app.setStyleSheet("""
        /* --- Brand Colors ---
           Orange:  #F07B3F
           Green:   #21BF73
           Blue:    #2D9CDB
           Dark BG: #1E1F24
           Card:    #2A2D34
           Border:  #3A3F47
           Text:    #E6E8EB
        */
        QWidget { 
            color: #E6E8EB; 
            font-family: "Inter", "Arial", sans-serif; 
            background-color: #1E1F24; 
        }

        /* Cards / Panels */
        QFrame.card {
            background-color: #2A2D34;
            border: 1px solid #3A3F47;
            border-radius: 12px;
        }

        /* Headings */
        QLabel[heading="true"] {
            font-size: 16px;
            font-weight: 700;
        }

        /* Buttons */
        QPushButton {
            background-color: #2D9CDB; /* Blue default */
            color: #FFFFFF;
            border: none;
            padding: 8px 14px;
            border-radius: 8px;
        }
        QPushButton:hover { background-color: #248AC3; }
        QPushButton:pressed { background-color: #1F79A9; }
        QPushButton:disabled { background-color: #3A3F47; color: #9AA3AC; }

        /* Primary action button */
        QPushButton#CopyButton { background-color: #F07B3F; font-weight: 700; }
        QPushButton#CopyButton:hover { background-color: #D96E39; }
        QPushButton#CopyButton:pressed { background-color: #C26134; }

        /* Checkboxes / Switch-like look */
        QCheckBox { spacing: 8px; }
        QCheckBox::indicator {
            width: 22px; height: 22px;
            border-radius: 5px;
            border: 1px solid #3A3F47;
            background: #1E1F24;
        }
        QCheckBox::indicator:checked { background: #21BF73; border-color: #21BF73; }
        QCheckBox::indicator:hover { border-color: #2D9CDB; }

        /* Line Edits */
        QLineEdit {
            background: #1E1F24; color: #E6E8EB;
            border: 1px solid #3A3F47; border-radius: 8px; padding: 6px 8px;
        }
        QLineEdit:focus { border-color: #2D9CDB; }

        /* ComboBox */
        QComboBox {
            background: #1E1F24; color: #E6E8EB;
            border: 1px solid #3A3F47; border-radius: 8px; padding: 6px 8px;
        }
        QComboBox QAbstractItemView { background: #1E1F24; color: #E6E8EB; selection-background-color: #2D9CDB; }

        /* Progress Bar */
        QProgressBar {
            border: 1px solid #3A3F47;
            border-radius: 6px;
            background: #1E1F24;
            text-align: center;
            color: #9AA3AC;
            height: 10px;
        }
        QProgressBar::chunk { background-color: #2D9CDB; border-radius: 6px; }

        /* Success / Warning / Error accents (via foreground color) */
        /* Applied programmatically; keeping palette alignment here */

        /* QTree (file list) */
        QTreeWidget {
            background: #1E1F24; color: #E6E8EB;
            border: 1px solid #3A3F47; border-radius: 8px;
        }
        QHeaderView::section {
            background: #2A2D34; color: #E6E8EB;
            border: none; padding: 6px; font-weight: 700;
        }
        QTreeWidget::item:selected { background: rgba(240, 123, 63, 0.25); }
        QTreeWidget::item:hover { background: rgba(45, 156, 219, 0.15); }

        /* Scrollbars */
        QScrollBar:vertical {
            background: #1E1F24; width: 10px; margin: 0px; border-radius: 5px;
        }
        QScrollBar::handle:vertical {
            background: #3A3F47; min-height: 20px; border-radius: 5px;
        }
        QScrollBar::handle:vertical:hover { background: #4A515A; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }

        QScrollBar:horizontal {
            background: #1E1F24; height: 10px; margin: 0px; border-radius: 5px;
        }
        QScrollBar::handle:horizontal {
            background: #3A3F47; min-width: 20px; border-radius: 5px;
        }
        QScrollBar::handle:horizontal:hover { background: #4A515A; }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }

        /* Labels with subtle secondary text */
        QLabel[secondary="true"] { color: #9AA3AC; }

        /* Drive speed label highlight (Green) */
        QLabel#DriveSpeed { color: #21BF73; font-weight: 600; }
    """)
    
    window = AutoCopierApp()
    window.show()
    sys.exit(app.exec())