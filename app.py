import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import os
import sys
import psutil # Added for disk space check
import traceback # Import traceback module
from datetime import datetime

# Import from our new modules
import config
import file_operations
import drive_manager

def get_drive_name_from_mountpoint(mountpoint):
    """Gets a clean, usable folder name from a drive's mountpoint."""
    if not mountpoint:
        return "Unknown_Drive"
    
    # For Windows, mountpoint might be 'D:\\'. We want 'D'.
    if sys.platform == "win32" and ":" in mountpoint:
        return mountpoint.split(":")[0]
    
    # For macOS/Linux, /Volumes/MyDisk or /media/user/MyDisk
    name = os.path.basename(mountpoint)
    return name if name else "Unknown_Drive"

class DriveWidget(ctk.CTkFrame):
    def __init__(self, master, mountpoint, description, selection_callback):
        super().__init__(master, fg_color="transparent")
        self.mountpoint = mountpoint
        self.is_selected = ctk.BooleanVar(value=False)
        self.selection_callback = selection_callback

        self.configure(fg_color=config.COLOR_FRAME, corner_radius=10)
        self.grid_columnconfigure(1, weight=1)

        # Checkbox for selection
        self.checkbox = ctk.CTkCheckBox(self, text="", variable=self.is_selected, 
                                        command=self.on_toggle, fg_color=config.COLOR_ACCENT_ORANGE)
        self.checkbox.grid(row=0, column=0, rowspan=2, padx=10, pady=5)

        # Drive name and description
        self.name_label = ctk.CTkLabel(self, text=get_drive_name_from_mountpoint(mountpoint), anchor="w", font=ctk.CTkFont(weight="bold"))
        self.name_label.grid(row=0, column=1, sticky="ew", padx=5, pady=(5,0))
        self.description_label = ctk.CTkLabel(self, text=description, anchor="w", text_color="gray")
        self.description_label.grid(row=1, column=1, sticky="ew", padx=5, pady=(0,5))

        # Progress bar
        self.progress_bar = ctk.CTkProgressBar(self, height=5, corner_radius=5, fg_color=config.COLOR_BG)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=2, column=0, columnspan=2, sticky="ew", padx=10, pady=(0, 5))

    def on_toggle(self):
        is_selected = self.is_selected.get()
        self.selection_callback(self.mountpoint, is_selected)

    def start_scan(self):
        self.progress_bar.configure(progress_color=config.COLOR_ACCENT_SKYBLUE)
        self.checkbox.configure(state="disabled")
        self.progress_bar.start()

    def finish_scan(self):
        self.progress_bar.stop()
        self.progress_bar.configure(progress_color=config.COLOR_STATUS_SUCCESS)
        self.checkbox.configure(state="normal")
        self.progress_bar.set(1)

    def reset(self):
        self.is_selected.set(False)
        self.progress_bar.stop()
        self.progress_bar.configure(progress_color=config.COLOR_ACCENT_SKYBLUE)
        self.progress_bar.set(0)
    
    def show_ejected_status(self):
        self.checkbox.configure(state="disabled")
        self.description_label.configure(text="✔ Đã tháo an toàn", text_color=config.COLOR_STATUS_SUCCESS)
        self.progress_bar.set(1)
        self.progress_bar.configure(progress_color=config.COLOR_STATUS_SUCCESS)

class AutoCopierApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # --- Window Setup ---
        self.title("Auto Video Copier Pro+")
        self.geometry("1100x900") # Increased height
        self.configure(fg_color=config.COLOR_BG)
        ctk.set_appearance_mode("Dark")

        # --- App State Variables ---
        self.destination_path = ctk.StringVar()
        self.delete_after_copy = ctk.BooleanVar(value=False)
        self.is_auto_mode = ctk.BooleanVar(value=False)
        
        # New settings variables
        self.file_extensions = ctk.StringVar()
        self.conflict_policy = ctk.StringVar()

        self.detected_drives = {}  # {mountpoint: {data}}
        self.video_item_map = {}   # {item_id: {data}}
        self.selected_drives = set() # To store mountpoints of selected drives
        self.drive_widgets = {}
        self.monitoring = True

        # --- Load and Build ---
        self.load_app_config()
        self.create_widgets()
        self.setup_treeview_style()
        
        # --- Start Background Processes ---
        self.monitor_thread = threading.Thread(target=self.monitor_drives_thread, daemon=True)
        self.monitor_thread.start()
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.after(100, self.update_drive_list)

    def _update_ui_states(self):
        """Central function to update the state of all interactive widgets."""
        is_auto = self.is_auto_mode.get()
        
        # Drive selection widgets are disabled in auto mode
        for widget in self.drive_widgets.values():
            widget.checkbox.configure(state="disabled" if is_auto else "normal")

        # File list action buttons state
        has_files_in_list = len(self.video_tree.get_children()) > 0
        can_select = has_files_in_list and not is_auto
        self.select_all_button.configure(state="normal" if can_select else "disabled")
        self.deselect_all_button.configure(state="normal" if can_select else "disabled")

        # Copy button state
        has_selection = len(self.video_tree.selection()) > 0
        can_copy = has_selection and not is_auto
        self.copy_button.configure(state="normal" if can_copy else "disabled")


    def on_closing(self):
        """Handle window close event."""
        self.monitoring = False
        self.destroy()

    def load_app_config(self):
        """Load configuration from file."""
        conf = config.load_config()
        self.destination_path.set(conf.get("destination_path", ""))
        self.file_extensions.set(conf.get("video_extensions", config.DEFAULT_VIDEO_EXTENSIONS))
        self.conflict_policy.set(conf.get("conflict_policy", config.DEFAULT_CONFLICT_POLICY))

    def save_app_config(self, *args):
        """Save the current settings to the config file."""
        conf_data = {
            "destination_path": self.destination_path.get(),
            "video_extensions": self.file_extensions.get(),
            "conflict_policy": self.conflict_policy.get()
        }
        config.save_config(conf_data)

    def browse_destination(self):
        """Open browse dialog and save selected path."""
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.destination_path.set(folder_selected)
            self.save_app_config()

    # --- UI Creation ---

    def create_widgets(self):
        """Build the main application UI."""
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self._create_left_panel()
        self._create_right_panel()

    def _create_left_panel(self):
        """Build the left panel for drive management."""
        left_panel = ctk.CTkFrame(self, fg_color=config.COLOR_FRAME, corner_radius=20, width=250)
        left_panel.grid(row=0, column=0, sticky='nsew', padx=15, pady=15)
        left_panel.grid_rowconfigure(1, weight=1)
        left_panel.grid_columnconfigure((0, 1), weight=1)
        left_panel.grid_propagate(False)
        
        ctk.CTkLabel(left_panel, text="Ổ Đĩa / Thẻ Nhớ", font=ctk.CTkFont(size=16, weight="bold"), text_color=config.COLOR_TEXT_HEADER).grid(row=0, column=0, columnspan=2, padx=10, pady=10, sticky='w')
        
        self.drive_list_frame = ctk.CTkScrollableFrame(left_panel, fg_color="transparent")
        self.drive_list_frame.grid(row=1, column=0, columnspan=2, sticky='nsew', padx=5)

        refresh_button = ctk.CTkButton(left_panel, text="Làm Mới", command=self.update_drive_list, fg_color=config.COLOR_ACCENT_SKYBLUE, text_color=config.COLOR_TEXT_HEADER)
        refresh_button.grid(row=2, column=0, columnspan=2, sticky='ew', padx=10, pady=10) # Make it span 2 columns

    def _create_right_panel(self):
        """Build the right panel for controls and video list."""
        right_panel = ctk.CTkFrame(self, fg_color="transparent")
        right_panel.grid(row=0, column=1, sticky='nsew', padx=(0, 15), pady=15)
        right_panel.grid_rowconfigure(2, weight=1) # Make row for tabs/log expand
        right_panel.grid_columnconfigure(0, weight=1)

        # --- Top controls frames (Destination, Mode, Settings) ---
        top_controls_frame = ctk.CTkFrame(right_panel, fg_color="transparent")
        top_controls_frame.grid(row=0, column=0, sticky='ew')
        top_controls_frame.grid_columnconfigure(0, weight=1)

        dest_frame = ctk.CTkFrame(top_controls_frame, fg_color=config.COLOR_FRAME, corner_radius=15)
        dest_frame.grid(row=0, column=0, sticky='ew', pady=(0, 5))
        dest_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(dest_frame, text="Thư Mục Đích:", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=3, padx=15, pady=(10,0), sticky='w')
        dest_entry = ctk.CTkEntry(dest_frame, textvariable=self.destination_path, state="readonly", fg_color="#2b2b2b")
        dest_entry.grid(row=1, column=0, columnspan=2, sticky='ew', padx=15, pady=5)
        browse_button = ctk.CTkButton(dest_frame, text="...", width=30, command=self.browse_destination, text_color=config.COLOR_TEXT_HEADER)
        browse_button.grid(row=1, column=2, padx=(5,15), pady=5)

        mode_frame = ctk.CTkFrame(top_controls_frame, fg_color=config.COLOR_FRAME, corner_radius=15)
        mode_frame.grid(row=0, column=1, sticky='ns', padx=(10,0))
        ctk.CTkLabel(mode_frame, text="Chế Độ", font=ctk.CTkFont(size=14, weight="bold")).pack(padx=15, pady=(10,5), anchor='w')
        mode_switch = ctk.CTkSwitch(mode_frame, text="Tự Động", variable=self.is_auto_mode, progress_color=config.COLOR_ACCENT_GREEN, command=self.toggle_auto_mode)
        mode_switch.pack(padx=15, pady=(0,10), anchor='w')

        settings_frame = ctk.CTkFrame(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15)
        settings_frame.grid(row=1, column=0, sticky='ew', pady=10)
        settings_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(settings_frame, text="Cài Đặt Nâng Cao", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=3, sticky='w', padx=15, pady=(10,5))
        ctk.CTkLabel(settings_frame, text="Các loại file:").grid(row=1, column=0, sticky='w', padx=15)
        ext_entry = ctk.CTkEntry(settings_frame, textvariable=self.file_extensions)
        ext_entry.grid(row=1, column=1, sticky='ew', padx=15, pady=5)
        ext_entry.bind("<KeyRelease>", self.save_app_config)
        ctk.CTkLabel(settings_frame, text="Khi file trùng:").grid(row=2, column=0, sticky='w', padx=15)
        conflict_menu = ctk.CTkOptionMenu(settings_frame, variable=self.conflict_policy,
                                           values=["Bỏ Qua", "Ghi Đè", "Đổi Tên"],
                                           fg_color=config.COLOR_FRAME, 
                                           button_color=config.COLOR_ACCENT_SKYBLUE,
                                           command=self.save_app_config)
        conflict_menu.grid(row=2, column=1, sticky='w', padx=15, pady=(5, 10))

        # --- Tab View for Files and Log ---
        self.tab_view = ctk.CTkTabview(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15,
                                  segmented_button_selected_color=config.COLOR_ACCENT_SKYBLUE)
        self.tab_view.grid(row=2, column=0, sticky='nsew', pady=10)
        self.tab_view.add("Danh Sách File")
        self.tab_view.add("Nhật Ký Hoạt Động")
        
        tree_frame = ctk.CTkFrame(self.tab_view.tab("Danh Sách File"), fg_color="transparent")
        tree_frame.pack(expand=True, fill="both")

        # Create Treeview with a new 'progress' column
        self.video_tree = ttk.Treeview(tree_frame, columns=("status", "name", "size", "drive", "time", "progress"), show="headings")
        self.video_tree.pack(side="left", fill="both", expand=True)

        tree_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.video_tree.yview)
        self.video_tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.pack(side="right", fill="y")

        self.log_textbox = ctk.CTkTextbox(self.tab_view.tab("Nhật Ký Hoạt Động"), state="disabled", fg_color=config.COLOR_EDITOR_BG, wrap="word", corner_radius=0, font=("Courier New", 12))
        self.log_textbox.pack(expand=True, fill="both", padx=10, pady=10)

        # --- Progress Bar Frame ---
        self.progress_frame = ctk.CTkFrame(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15)
        self.progress_frame.grid(row=3, column=0, sticky='ew', pady=10)
        self.progress_frame.grid_columnconfigure(0, weight=1)
        self.progress_status_label = ctk.CTkLabel(self.progress_frame, text="Sẵn sàng", anchor='w')
        self.progress_status_label.grid(row=0, column=0, sticky='ew', padx=15, pady=5)
        self.progress_bar = ctk.CTkProgressBar(self.progress_frame, progress_color=config.COLOR_ACCENT_SKYBLUE)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=1, column=0, sticky='ew', padx=15, pady=(0, 10))
        self.progress_frame.grid_remove() # Hide it initially

        # --- Action Buttons ---
        action_frame = ctk.CTkFrame(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15)
        action_frame.grid(row=4, column=0, sticky='ew')
        action_frame.grid_columnconfigure(3, weight=1)
        self.select_all_button = ctk.CTkButton(action_frame, text="Chọn Tất Cả", command=self.select_all_videos, state="disabled", text_color=config.COLOR_TEXT_HEADER)
        self.select_all_button.grid(row=0, column=0, padx=10, pady=10)
        self.deselect_all_button = ctk.CTkButton(action_frame, text="Bỏ Chọn Tất Cả", command=self.deselect_all_videos, state="disabled", text_color=config.COLOR_TEXT_HEADER)
        self.deselect_all_button.grid(row=0, column=1, padx=5, pady=10)
        self.delete_checkbox = ctk.CTkCheckBox(action_frame, text="Xóa sạch thẻ sau khi chép", variable=self.delete_after_copy, fg_color=config.COLOR_ACCENT_GREEN, hover_color=config.COLOR_ACCENT_GREEN)
        self.delete_checkbox.grid(row=0, column=2, padx=20, pady=10)
        self.copy_button = ctk.CTkButton(action_frame, text="Sao Chép Thủ Công", command=self.start_manual_copy, state="disabled", fg_color=config.COLOR_ACCENT_ORANGE, font=ctk.CTkFont(weight="bold"), text_color=config.COLOR_TEXT_HEADER)
        self.copy_button.grid(row=0, column=4, sticky='e', padx=10, pady=10)

    def setup_treeview_style(self):
        """Configure the style for the Treeview widget."""
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background=config.COLOR_EDITOR_BG, foreground=config.COLOR_TEXT, rowheight=28, fieldbackground=config.COLOR_EDITOR_BG, borderwidth=0, relief="flat")
        style.map('Treeview', background=[('selected', config.COLOR_ACCENT_ORANGE)])
        style.configure("Treeview.Heading", font=('Arial', 13, 'bold'), background="#313335", foreground=config.COLOR_TEXT_HEADER, relief="flat")
        style.map("Treeview.Heading", background=[('active', '#3a3c3e')])
        style.configure('Treeview.Cell', padding=4)
        style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})]) # Remove borders

        self.video_tree.heading("status", text="Trạng Thái")
        self.video_tree.heading("name", text="Tên Tệp")
        self.video_tree.heading("size", text="Kích Thước")
        self.video_tree.heading("drive", text="Thiết Bị")
        self.video_tree.heading("time", text="Thời Gian")
        self.video_tree.heading("progress", text="Tiến Trình")
        self.video_tree.column("status", width=120, anchor='w')
        self.video_tree.column("name", width=350, anchor='w')
        self.video_tree.column("size", width=100, anchor='center')
        self.video_tree.column("drive", width=120, anchor='w')
        self.video_tree.column("time", width=80, anchor='center')
        self.video_tree.column("progress", width=150, anchor='w')
        self.video_tree.tag_configure('pending', foreground=config.COLOR_TEXT)
        self.video_tree.tag_configure('processing', foreground=config.COLOR_STATUS_WARN)
        self.video_tree.tag_configure('success', foreground=config.COLOR_STATUS_SUCCESS)
        self.video_tree.tag_configure('error', foreground=config.COLOR_STATUS_ERROR)

    def log_message(self, message, level="INFO"):
        """Adds a message to the log text box."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = "[{}] [{}]: {}\n".format(timestamp, level, message)
        self.log_textbox.configure(state="normal")
        self.log_textbox.insert("end", formatted_message)
        self.log_textbox.configure(state="disabled")
        self.log_textbox.see(tk.END)

    # --- Core Logic and Event Handlers ---

    def toggle_auto_mode(self):
        is_auto = self.is_auto_mode.get()
        if is_auto:
            self.log_message("Chế độ TỰ ĐỘNG đã được BẬT.", level="INFO")
            # Uncheck all drive widgets and clear selections
            for widget in self.drive_widgets.values():
                if widget.is_selected.get():
                    widget.reset()
            self.selected_drives.clear()
            self.clear_video_list()
            
            self.log_message(f"Tự động: Bắt đầu xử lý {len(self.drive_widgets)} thẻ hiện có...", level="INFO")
            for mountpoint in self.drive_widgets.keys():
                self.start_auto_process(mountpoint)
        else:
            self.log_message("Chế độ TỰ ĐỘNG đã được TẮT.", level="INFO")
        
        self._update_ui_states()

    def monitor_drives_thread(self):
        """Monitors for drive changes and handles auto-processing if enabled."""
        known_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
        while self.monitoring:
            try:
                current_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
                new_drives = current_mountpoints - known_mountpoints
                removed_drives = known_mountpoints - current_mountpoints

                if new_drives or removed_drives:
                    self.after(0, self.update_drive_list)
                    if self.is_auto_mode.get():
                        for mountpoint in new_drives:
                            self.log_message(f"Tự động: Phát hiện thẻ mới {mountpoint}. Bắt đầu xử lý.", level="INFO")
                            self.start_auto_process(mountpoint)
                
                known_mountpoints = current_mountpoints
            except Exception as e:
                print(f"Error in monitor thread: {e}")
            time.sleep(3)

    def update_drive_list(self):
        """Refresh the list of drives in the left panel."""
        current_drives = drive_manager.get_removable_drives()
        current_mountpoints = {d.mountpoint for d in current_drives}
        existing_mountpoints = set(self.drive_widgets.keys())

        # Remove widgets for drives that are no longer connected
        for mountpoint in existing_mountpoints - current_mountpoints:
            widget = self.drive_widgets.pop(mountpoint)
            widget.destroy()
            if mountpoint in self.selected_drives:
                self.selected_drives.remove(mountpoint)
                self.clear_video_list_for_drive(mountpoint)

        # Add widgets for new drives
        for drive in current_drives:
            mountpoint = drive.mountpoint
            if mountpoint not in self.drive_widgets:
                drive_label = get_drive_name_from_mountpoint(mountpoint)
                # A simple description from the drive label and file system type
                description = f"{drive_label} ({drive.fstype})" if drive_label and drive_label != "/" else f"{drive.device} ({drive.fstype})"
                widget = DriveWidget(self.drive_list_frame, mountpoint, description, self.on_drive_selection_changed)
                widget.pack(fill='x', expand=True, pady=(0, 5))
                self.drive_widgets[mountpoint] = widget

        self.detected_drives = {d.mountpoint: {'device': d.device, 'description': d.opts} for d in current_drives}


    def on_drive_selection_changed(self, mountpoint, is_selected):
        if self.is_auto_mode.get(): 
            # In auto mode, selection is automatic and does not depend on user input
            return

        if is_selected:
            if mountpoint not in self.selected_drives:
                self.selected_drives.add(mountpoint)
                widget = self.drive_widgets.get(mountpoint)
                if widget:
                    widget.start_scan()
                # self.list_videos_from_drive(mountpoint)
                # Use a thread to avoid blocking the UI, especially if multiple drives are selected quickly
                threading.Thread(target=self.list_videos_from_drive, args=(mountpoint,), daemon=True).start()
                # self.eject_button.configure(state="normal") # Enable eject if any drive is selected - REMOVED
        else:
            if mountpoint in self.selected_drives:
                self.selected_drives.remove(mountpoint)
                self.clear_video_list_for_drive(mountpoint)
                # Disable eject if no drives are selected - REMOVED
                # if not self.selected_drives:
                #     self.eject_button.configure(state="disabled")
        
        self._update_ui_states()

    def _start_eject_drive(self, mountpoint_to_eject):
        drive_name = get_drive_name_from_mountpoint(mountpoint_to_eject)
        
        # No more confirmation, just log and eject.
        self.log_message(f"Bắt đầu tự động tháo {drive_name}.", level="INFO")
        
        # Check if drive still exists before trying to eject
        if mountpoint_to_eject not in self.detected_drives:
            self.log_message(f"Không thể tháo {drive_name}, thiết bị không còn được kết nối.", "WARN")
            self.update_drive_list()
            return

        threading.Thread(target=self._eject_drive_thread, args=(mountpoint_to_eject,), daemon=True).start()

    def _eject_drive_thread(self, mountpoint):
        """Worker thread for ejecting a single drive."""
        widget = self.drive_widgets.get(mountpoint)
        drive_name = get_drive_name_from_mountpoint(mountpoint)

        self.after(0, lambda: self.log_message(f"Đang tháo {drive_name}...", level="INFO"))
        drive_info = self.detected_drives.get(mountpoint)
        
        if not drive_info:
            self.after(0, lambda: self.log_message(f"Lỗi khi tháo {drive_name}: Không tìm thấy thông tin thiết bị.", level="ERROR"))
            return
        
        success, message = drive_manager.eject_drive(drive_info['device'])
        
        if success:
            self.after(0, lambda: self.log_message(f"Đã tháo thành công {drive_name}.", level="SUCCESS"))
            if widget:
                # Update the specific widget to show its final ejected status
                self.after(0, widget.show_ejected_status)
        else:
            self.after(0, lambda: self.log_message(f"Lỗi khi tháo {drive_name}: {message}", level="ERROR"))
        
        # The main monitoring thread will handle the drive disappearing from the list.
        # We can trigger a manual refresh to speed it up.
        self.after(1000, self.update_drive_list)


    def list_videos_from_drive(self, drive_path):
        """Initiates the video search for a specific drive."""
        widget = self.drive_widgets.get(drive_path)
        if not widget: return # Should not happen if drive_path is valid

        self.log_message(f"Đang tìm kiếm file trên {get_drive_name_from_mountpoint(drive_path)}...", level="INFO")
        # Check if drive still exists before starting the thread
        if drive_path not in self.detected_drives:
            self.log_message(f"Hủy quét: {get_drive_name_from_mountpoint(drive_path)} không còn được kết nối.", "WARN")
            widget.finish_scan() # Mark as 'done' even if it disappeared
            return
        threading.Thread(target=self._search_videos_thread, args=(drive_path,), daemon=True).start()
    
    def clear_video_list_for_drive(self, mountpoint_to_clear):
        """Clears videos from the Treeview that belong to a specific drive."""
        drive_name_to_clear = get_drive_name_from_mountpoint(mountpoint_to_clear)
        items_to_delete = []
        for item_id in self.video_tree.get_children():
            item_drive_name = self.video_tree.item(item_id, "values")[3]
            if item_drive_name == drive_name_to_clear:
                items_to_delete.append(item_id)
        
        for item_id in items_to_delete:
            self.video_tree.delete(item_id)
            if item_id in self.video_item_map:
                del self.video_item_map[item_id]
        
        self._update_ui_states()
        self.log_message(f"Đã xóa file từ {drive_name_to_clear} khỏi danh sách.", level="INFO")

    def _search_videos_thread(self, drive_path):
        """Worker thread to find videos and add them to the UI as they are found."""
        try:
            extensions = self.file_extensions.get()
            
            new_item_ids = []
            any_files_found = False
            for video_data in file_operations.find_files_on_drive(drive_path, extensions):
                if not self.monitoring: return
                any_files_found = True
                # Use a queue-like approach to add items in the main thread
                item_id = self.add_video_to_list(video_data)
                if item_id:
                    new_item_ids.append(item_id)

            # After the loop, finish the scan on the widget
            if drive_path in self.drive_widgets:
                self.after(0, lambda w=self.drive_widgets[drive_path]: w.finish_scan())
            
            # After a short delay, select all the newly added items
            if new_item_ids:
                self.after(100, lambda: self.video_tree.selection_add(new_item_ids))
            
            if not any_files_found:
                self.after(0, self.show_no_videos_found, drive_path)
            
            self.after(100, self._update_ui_states)

        except Exception as e:
            print(f"Error in search thread for {drive_path}: {e}")
            # Use lambda to fix TypeError
            self.after(0, lambda: self.log_message(f"Lỗi khi quét file trên {drive_path}: {e}", level="ERROR"))

    def select_all_videos(self):
        """Selects all video items in the treeview."""
        self.video_tree.selection_add(*self.video_tree.get_children())
        self._update_ui_states()

    def deselect_all_videos(self):
        """Deselects all video items in the treeview."""
        self.video_tree.selection_remove(*self.video_tree.get_children())
        self._update_ui_states()

    def update_copy_button_state(self):
        """Enable or disable copy button based on selections."""
        self._update_ui_states()

    def start_manual_copy(self):
        """Starts the manual copy process for all selected files from all selected drives."""
        selected_item_ids = self.video_tree.selection()
        if not selected_item_ids:
            messagebox.showwarning("Chưa chọn file", "Vui lòng chọn ít nhất một file để sao chép.")
            return

        if not self.destination_path.get():
            messagebox.showwarning("Chưa chọn đích", "Vui lòng chọn thư mục đích trước khi sao chép.")
            return
        
        # Group videos by the drive they belong to
        videos_by_drive = {}
        item_ids_by_drive = {}
        for item_id in selected_item_ids:
            video_info = self.video_item_map.get(item_id)
            if not video_info: continue
            
            mountpoint = video_info['drive']
            if mountpoint not in videos_by_drive:
                videos_by_drive[mountpoint] = []
                item_ids_by_drive[mountpoint] = []
            
            videos_by_drive[mountpoint].append(video_info)
            item_ids_by_drive[mountpoint].append(item_id)

        # A single confirmation for all drives if wiping is enabled
        if self.delete_after_copy.get():
            num_drives = len(videos_by_drive)
            drive_names = ", ".join([get_drive_name_from_mountpoint(mp) for mp in videos_by_drive.keys()])
            confirm_message = (f"Bạn có chắc chắn muốn XÓA VĨNH VIỄN TOÀN BỘ DỮ LIỆU "
                               f"trên {num_drives} thẻ ({drive_names}) sau khi sao chép thành công không?")
            if not messagebox.askyesno("Xác Nhận Xóa Sạch Thẻ", confirm_message, icon='warning'):
                return

        # Start a separate copy process for each drive
        for mountpoint, videos in videos_by_drive.items():
            item_ids = item_ids_by_drive[mountpoint]
            self.start_copy_process(mountpoint, videos, item_ids)


    def start_auto_process(self, mountpoint):
        """Starts the automatic process for a single drive in a new thread."""
        if not self.is_auto_mode.get(): return
        widget = self.drive_widgets.get(mountpoint)
        if widget:
            widget.start_scan()
        threading.Thread(target=self._auto_process_thread, args=(mountpoint,), daemon=True).start()

    def _auto_process_thread(self, mountpoint):
        """The actual worker thread for the automatic process. No UI updates here."""
        try:
            # Use lambda to fix TypeError
            self.after(0, lambda: self.log_message(f"Tự động: Đang quét {mountpoint}...", level="INFO"))
            extensions = self.file_extensions.get()
            
            videos_to_process = list(file_operations.find_files_on_drive(mountpoint, extensions))

            widget = self.drive_widgets.get(mountpoint)
            if widget:
                self.after(0, widget.finish_scan)

            if not videos_to_process:
                # Use lambda to fix TypeError
                self.after(0, lambda: self.log_message(f"Tự động: Không tìm thấy file phù hợp trên {mountpoint}.", level="INFO"))
                return

            # Use lambda to fix TypeError
            self.after(0, lambda: self.log_message(f"Tự động: Tìm thấy {len(videos_to_process)} file trên {mountpoint}. Bắt đầu sao chép.", level="INFO"))
            
            self.start_copy_process(mountpoint, videos_to_process, item_ids=None)
        except Exception as e:
            print(f"Error in auto process thread for {mountpoint}: {e}")
            # Use lambda to fix TypeError
            self.after(0, lambda: self.log_message(f"Lỗi khi tự động quét file trên {mountpoint}: {e}", level="ERROR"))

    def start_copy_process(self, mountpoint, videos_to_process, item_ids):
        """Generic copy process starter for both auto and manual modes."""
        destination_root = self.destination_path.get()
        if not destination_root or not os.path.isdir(destination_root):
            messagebox.showerror("Lỗi", "Vui lòng chọn một thư mục đích hợp lệ.")
            return
        
        # --- Disk Space Check ---
        files_to_copy_paths = [v['path'] for v in videos_to_process]
        required_space = file_operations.get_required_space(files_to_copy_paths)
        
        # The logic to pre-create a destination folder here was flawed for manual mode
        # and redundant for automatic mode. The worker thread already handles 
        # creating the correct subdirectories for each file. This logic is removed.
        
        if not file_operations.has_enough_space(destination_root, required_space):
            messagebox.showerror("Thiếu Dung Lượng", 
                                 "Không đủ dung lượng trống tại '{}'.\nCần: {:.2f} GB\nCòn lại: {:.2f} GB".format(
                                     destination_root, 
                                     required_space / (1024**3), 
                                     psutil.disk_usage(destination_root).free / (1024**3)))
            return

        # Folder creation is now handled reliably inside the worker thread for each file.
        # This prevents the creation of a useless "Manual" folder and potential conflicts.
        
        # Confirmation is now handled in the calling functions (start_manual_copy)
        # For auto-mode, we assume consent if the wipe checkbox is ticked.
        if self.is_auto_mode.get() and self.delete_after_copy.get():
             drive_name = get_drive_name_from_mountpoint(mountpoint)
             self.log_message(f"Cảnh báo: Thẻ {drive_name} sẽ bị xóa sạch sau khi sao chép.", "WARN")

        # Show progress bar
        self.progress_frame.grid()
        self.progress_bar.set(0)
        self.progress_status_label.configure(text="Chuẩn bị sao chép...")
        self.log_message("Bắt đầu quá trình sao chép cho {}...".format(get_drive_name_from_mountpoint(mountpoint) if mountpoint != 'Manual' else 'các file đã chọn'))

        threading.Thread(target=self._copy_process_thread, 
                         args=(videos_to_process, destination_root, self.delete_after_copy.get(), item_ids, mountpoint), 
                         daemon=True).start()

    def _copy_process_thread(self, videos, destination_root, should_wipe, item_ids, process_id):
        """The main worker thread for copying, verifying, and deleting."""
        try:
            final_results = {"success": 0, "error": 0, "skipped": 0}
            conflict_policy = self.conflict_policy.get()
            total_files = len(videos)
            
            is_manual_mode = (item_ids is not None)

            for i, video_info in enumerate(videos):
                start_time = time.time()
                source_path = video_info['path']
                file_name = os.path.basename(source_path)

                # Determine the correct subfolder (drive name)
                # In our new logic, process_id is always the mountpoint
                drive_name = get_drive_name_from_mountpoint(process_id)

                final_destination_folder = os.path.join(destination_root, drive_name)
                try:
                    os.makedirs(final_destination_folder, exist_ok=True)
                except OSError as e:
                    self.after(0, lambda: self.log_message(f"Không thể tạo thư mục {final_destination_folder}: {e}", "ERROR"))
                    final_results["error"] += 1
                    continue # Skip this file

                # Update overall progress
                progress_value = (i + 1) / total_files
                progress_text = f"Đang xử lý {i+1}/{total_files}: {file_name}"
                self.after(0, lambda p=progress_value: self.progress_bar.set(p))
                self.after(0, lambda t=progress_text: self.progress_status_label.configure(text=t))

                # Define status callback to handle detailed progress
                def status_callback(status_key, status_text, progress_value=None):
                    if is_manual_mode:
                        # Ensure index is within bounds
                        if i < len(item_ids):
                            item_id = item_ids[i]
                            if self.video_tree.exists(item_id):
                                self.after(0, self.update_item_status, item_id, status_text, status_key, progress_value)
                    # Use a lambda to correctly pass the keyword argument to log_message via self.after
                    self.after(0, lambda: self.log_message(f"{file_name}: {status_text}", level=status_key.upper()))

                # Perform the core operation
                try:
                    success, skipped = file_operations.copy_and_verify_file(source_path, final_destination_folder, conflict_policy, status_callback)
                    if success:
                        final_results["success"] += 1
                        if skipped:
                            final_results["skipped"] += 1
                    else:
                        final_results["error"] += 1
                except Exception as e:
                    final_results["error"] += 1
                    status_callback("error", f"Lỗi nghiêm trọng: {e}", -1.0)
                
                # After processing, calculate duration and update UI
                duration = time.time() - start_time
                if is_manual_mode:
                    if i < len(item_ids):
                        item_id = item_ids[i]
                        if self.video_tree.exists(item_id):
                            self.after(0, self.update_item_time, item_id, duration)
            
            # --- Finalize ---
            all_files_processed_successfully = final_results["error"] == 0
            mountpoint = process_id # process_id is the mountpoint for the current task
            drive_name_for_report = get_drive_name_from_mountpoint(mountpoint)

            # If all files were copied without errors, proceed to wipe and/or eject
            if all_files_processed_successfully:
                if should_wipe:
                    def wipe_status_callback(status_key, status_text, progress_value=None):
                        self.after(0, lambda: self.log_message(f"Xóa thẻ {drive_name_for_report}: {status_text}", level=status_key.upper()))
                    
                    self.after(0, lambda: self.log_message(f"Bắt đầu xóa sạch thẻ {drive_name_for_report}...", "INFO"))
                    wipe_success, wipe_message = file_operations.wipe_drive_data(mountpoint, wipe_status_callback)
                    self.after(0, lambda: self.log_message(f"Kết quả xóa thẻ {drive_name_for_report}: {wipe_message}", "SUCCESS" if wipe_success else "ERROR"))

                # Always eject automatically after a successful process for a drive
                self._start_eject_drive(mountpoint)

            # Schedule the single finalization function to run on the main thread
            self.after(0, self._finalize_copy_process, final_results, drive_name_for_report)
        
        except Exception as e:
            tb_str = traceback.format_exc()
            self.after(0, self._show_thread_error, "_copy_process_thread", tb_str)

    def _finalize_copy_process(self, results, drive_name):
        """Handles all UI updates after a copy process is complete."""
        # Hide progress bar and update status
        self.progress_frame.grid_remove()
        self.log_message(f"Hoàn tất quá trình cho {drive_name}.")
        self.log_message(f"Báo cáo: Thành công: {results['success'] - results['skipped']}, Bỏ qua: {results['skipped']}, Lỗi: {results['error']}", "INFO")

        # Show the final pop-up report after a small delay to prevent UI conflicts on macOS
        self.after(100, lambda: self.show_final_report(results, drive_name))

        # Switch view to the file list so the user can see the detailed status
        self.tab_view.set("Danh Sách File")

        # Bring the window to the front to make sure the user sees the result
        self.lift()
        self.attributes('-topmost', True)
        self.after(200, lambda: self.attributes('-topmost', False))
        
        # Update button states
        self._update_ui_states()
        
    # --- UI Helpers ---

    def add_video_to_list(self, video_data):
        """Add a single video file to the treeview and returns the item ID."""
        if not self.monitoring: return None
        try:
            size_mb = video_data['size'] / (1024*1024)
            drive_name = get_drive_name_from_mountpoint(video_data.get('drive', 'N/A'))
            
            item_id = self.video_tree.insert("", "end", values=(
                "Sẵn sàng",
                os.path.basename(video_data['path']),
                f"{size_mb:.2f} MB",
                drive_name,
                "", # Placeholder for time
                "" # Placeholder for progress
            ))

            self.video_item_map[item_id] = { "path": video_data['path'], "drive": video_data['drive'] }
            # Schedule a UI update after adding an item
            self.after(50, self._update_ui_states)
            return item_id
        except Exception as e:
            print(f"Error adding video to list for {video_data.get('path', 'N/A')}: {e}")
            return None

    def update_item_time(self, item_id, duration_seconds):
        """Updates the time column for a specific item in the treeview."""
        try:
            self.video_tree.set(item_id, "time", f"{duration_seconds:.2f} s")
        except tk.TclError:
            print(f"Could not update time for item {item_id} (it may have been deleted).")

    def update_item_status(self, item_id, status_text, tag, progress_value=None):
        """Updates the status and progress bar for a specific item."""
        try:
            # Update status text
            final_text = status_text
            if tag == 'success' and 'Hoàn thành' in status_text:
                final_text = f"✔ {status_text}"
            self.video_tree.set(item_id, "status", final_text)

            # Update progress bar visualization
            if progress_value is not None:
                if progress_value == -1.0: # Error case
                    progress_bar = "[!!! LỖI !!!]"
                else:
                    bar_length = 10
                    filled_length = int(bar_length * progress_value)
                    bar = '█' * filled_length + '-' * (bar_length - filled_length)
                    progress_bar = f"[{bar}] {int(progress_value * 100)}%"
                self.video_tree.set(item_id, "progress", progress_bar)
            
            # Update color tag
            self.video_tree.item(item_id, tags=(tag,))
        except tk.TclError:
            print(f"Could not update status for item {item_id} (it may have been deleted).")

    def show_no_videos_found(self, drive_path):
        self.log_message(f"Không tìm thấy file video nào phù hợp trên {get_drive_name_from_mountpoint(drive_path)}.", "WARN")

    def show_final_report(self, results, drive_name):
        """Display a summary report after a process finishes."""
        message = "Hoàn tất xử lý cho thẻ nhớ: {}\n\nThành công: {} tệp\nBỏ qua: {} tệp\nThất bại: {} tệp".format(drive_name, results['success'] - results['skipped'], results['skipped'], results['error'])
        if results["error"] > 0:
            messagebox.showwarning("Báo Cáo", message)
        else:
            messagebox.showinfo("Báo Cáo", message)

    def clear_video_list(self):
        self.video_tree.delete(*self.video_tree.get_children())
        self.video_item_map.clear()
        self._update_ui_states()

    def _show_thread_error(self, thread_name, error_details):
        """Displays a critical error from a background thread in a messagebox."""
        self.log_message(f"Lỗi nghiêm trọng trong luồng {thread_name}: {error_details}", "CRITICAL")
        # Schedule the messagebox to avoid calling it during a UI draw event on macOS
        self.after(100, lambda: messagebox.showerror(f"Lỗi Luồng {thread_name}", 
                             f"Đã xảy ra lỗi không mong muốn. Vui lòng báo cáo lỗi này:\n\n{error_details}"))
        # Also hide the progress bar to signal completion
        self.progress_frame.grid_remove()
