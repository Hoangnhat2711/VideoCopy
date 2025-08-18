import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import os
import sys
import psutil # Added for disk space check
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
    def __init__(self, master, mountpoint, description, selection_callback, eject_callback):
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

        # Eject button (initially hidden)
        self.eject_button = ctk.CTkButton(self, text="Tháo", width=40, height=20, text_color=config.COLOR_TEXT_HEADER,
                                          fg_color=config.COLOR_ACCENT_SKYBLUE, command=lambda: eject_callback(self.mountpoint))
        self.eject_button.grid(row=0, column=2, rowspan=2, padx=(0, 10))
        self.eject_button.grid_remove() # Hide it

        # Progress bar
        self.progress_bar = ctk.CTkProgressBar(self, height=5, corner_radius=5, fg_color=config.COLOR_BG)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=2, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 5))

    def on_toggle(self):
        is_selected = self.is_selected.get()
        if is_selected:
            self.eject_button.grid() # Show eject button
        else:
            self.eject_button.grid_remove() # Hide eject button
        self.selection_callback(self.mountpoint, is_selected)

    def start_scan(self):
        self.progress_bar.configure(progress_color=config.COLOR_ACCENT_SKYBLUE)
        self.eject_button.configure(state="disabled")
        self.progress_bar.start()

    def finish_scan(self):
        self.progress_bar.stop()
        self.progress_bar.configure(progress_color=config.COLOR_STATUS_SUCCESS)
        self.eject_button.configure(state="normal")
        self.progress_bar.set(1)

    def reset(self):
        self.is_selected.set(False)
        self.progress_bar.stop()
        self.progress_bar.configure(progress_color=config.COLOR_ACCENT_SKYBLUE)
        self.progress_bar.set(0)
        self.eject_button.grid_remove()

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
        tab_view = ctk.CTkTabview(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15,
                                  segmented_button_selected_color=config.COLOR_ACCENT_SKYBLUE)
        tab_view.grid(row=2, column=0, sticky='nsew', pady=10)
        tab_view.add("Danh Sách File")
        tab_view.add("Nhật Ký Hoạt Động")
        
        tree_frame = ctk.CTkFrame(tab_view.tab("Danh Sách File"), fg_color="transparent")
        tree_frame.pack(expand=True, fill="both")

        # Create Treeview with a new 'drive' column
        self.video_tree = ttk.Treeview(tree_frame, columns=("status", "name", "size", "drive"), show="headings")
        self.video_tree.pack(side="left", fill="both", expand=True)

        tree_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.video_tree.yview)
        self.video_tree.configure(yscrollcommand=tree_scroll.set)
        tree_scroll.pack(side="right", fill="y")

        self.log_textbox = ctk.CTkTextbox(tab_view.tab("Nhật Ký Hoạt Động"), state="disabled", fg_color=config.COLOR_EDITOR_BG, wrap="word", corner_radius=0, font=("Courier New", 12))
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
        self.delete_checkbox = ctk.CTkCheckBox(action_frame, text="Tự động xóa file gốc", variable=self.delete_after_copy, fg_color=config.COLOR_ACCENT_GREEN, hover_color=config.COLOR_ACCENT_GREEN)
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
        self.video_tree.heading("drive", text="Thiết Bị") # New column
        self.video_tree.column("status", width=120, anchor='w')
        self.video_tree.column("name", width=400, anchor='w')
        self.video_tree.column("size", width=100, anchor='center')
        self.video_tree.column("drive", width=150, anchor='w') # New column config
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
                widget = DriveWidget(self.drive_list_frame, mountpoint, description, self.on_drive_selection_changed, self.eject_active_drive)
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
                self.list_videos_from_drive(mountpoint)
                # self.eject_button.configure(state="normal") # Enable eject if any drive is selected - REMOVED
        else:
            if mountpoint in self.selected_drives:
                self.selected_drives.remove(mountpoint)
                self.clear_video_list_for_drive(mountpoint)
                # Disable eject if no drives are selected - REMOVED
                # if not self.selected_drives:
                #     self.eject_button.configure(state="disabled")
        
        self._update_ui_states()

    def eject_active_drive(self, mountpoint_to_eject):
        drive_name = get_drive_name_from_mountpoint(mountpoint_to_eject)
        confirmation = messagebox.askyesno("Xác Nhận Tháo Thiết Bị", 
                                           f"Bạn có chắc chắn muốn tháo an toàn thiết bị {drive_name} không?",
                                           icon='warning')
        
        if not confirmation:
            self.log_message(f"Đã hủy thao tác tháo {drive_name}.", level="INFO")
            return
        
        # Check if drive still exists before trying to eject
        if mountpoint_to_eject not in self.detected_drives:
            messagebox.showerror("Lỗi", f"Thiết bị {drive_name} không còn được kết nối.")
            self.update_drive_list()
            return

        threading.Thread(target=self._eject_drives_thread, args=([mountpoint_to_eject],), daemon=True).start()

    def _eject_drives_thread(self, drives_to_eject):
        """Worker thread for ejecting a list of drives (now usually just one)."""
        # Disable the specific widget's eject button
        for mountpoint in drives_to_eject:
            widget = self.drive_widgets.get(mountpoint)
            if widget:
                self.after(0, lambda w=widget: w.eject_button.configure(state="disabled"))

        self.after(0, self.log_message, f"Bắt đầu quá trình tháo {len(drives_to_eject)} thiết bị...", level="INFO")

        success_count = 0
        error_details = []

        for mountpoint in drives_to_eject:
            self.after(0, self.log_message, f"Đang tháo {get_drive_name_from_mountpoint(mountpoint)}...", level="INFO")
            drive_info = self.detected_drives.get(mountpoint)
            if not drive_info:
                error_details.append(f" - {get_drive_name_from_mountpoint(mountpoint)}: Không tìm thấy thông tin thiết bị.")
                self.after(0, self.log_message, f"Lỗi khi tháo {get_drive_name_from_mountpoint(mountpoint)}: Không tìm thấy thông tin.", level="ERROR")
                continue
            
            success, message = drive_manager.eject_drive(drive_info['device'])
            if success:
                success_count += 1
                self.after(0, self.log_message, f"Đã tháo thành công {get_drive_name_from_mountpoint(mountpoint)}.", level="SUCCESS")
            else:
                error_details.append(f" - {get_drive_name_from_mountpoint(mountpoint)}: {message}")
                self.after(0, self.log_message, f"Lỗi khi tháo {get_drive_name_from_mountpoint(mountpoint)}: {message}", level="ERROR")
        
        # Final Report
        report_message = f"Hoàn tất quá trình tháo thiết bị.\n\nThành công: {success_count}/{len(drives_to_eject)}"
        if error_details:
            report_message += "\n\nChi tiết lỗi:\n" + "\n".join(error_details)
        
        self.after(0, messagebox.showinfo, "Báo Cáo Tháo Thiết Bị", report_message)
        self.after(0, self.update_drive_list)
        self.after(0, self._update_ui_states)


    def list_videos_from_drive(self, drive_path):
        """Initiates the video search for a specific drive."""
        self.log_message(f"Đang tìm kiếm file trên {get_drive_name_from_mountpoint(drive_path)}...", level="INFO")
        # Check if drive still exists before starting the thread
        if drive_path not in self.detected_drives:
            self.log_message(f"Hủy quét: {get_drive_name_from_mountpoint(drive_path)} không còn được kết nối.", "WARN")
            widget = self.drive_widgets.get(drive_path)
            if widget:
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
                self.after(0, self.drive_widgets[drive_path].finish_scan)
            
            # After a short delay, select all the newly added items
            if new_item_ids:
                self.after(100, lambda: self.video_tree.selection_add(new_item_ids))
            
            if not any_files_found:
                self.after(0, self.show_no_videos_found, drive_path)
            
            self.after(100, self._update_ui_states)

        except Exception as e:
            print(f"Error in search thread for {drive_path}: {e}")
            self.after(0, self.log_message, f"Lỗi khi quét file trên {drive_path}: {e}", level="ERROR")

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

        videos_to_process = [self.video_item_map[item_id] for item_id in selected_item_ids]
        
        if not self.destination_path.get():
            messagebox.showwarning("Chưa chọn đích", "Vui lòng chọn thư mục đích trước khi sao chép.")
            return

        self.start_copy_process('Manual', videos_to_process, selected_item_ids)


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
            self.after(0, self.log_message, f"Tự động: Đang quét {mountpoint}...", level="INFO")
            extensions = self.file_extensions.get()
            
            videos_to_process = list(file_operations.find_files_on_drive(mountpoint, extensions))

            widget = self.drive_widgets.get(mountpoint)
            if widget:
                self.after(0, widget.finish_scan)

            if not videos_to_process:
                self.after(0, self.log_message, f"Tự động: Không tìm thấy file phù hợp trên {mountpoint}.", level="INFO")
                return

            self.after(0, self.log_message, f"Tự động: Tìm thấy {len(videos_to_process)} file trên {mountpoint}. Bắt đầu sao chép.", level="INFO")
            
            self.start_copy_process(mountpoint, videos_to_process, item_ids=None)
        except Exception as e:
            print(f"Error in auto process thread for {mountpoint}: {e}")
            self.after(0, self.log_message, f"Lỗi khi tự động quét file trên {mountpoint}: {e}", level="ERROR")

    def start_copy_process(self, mountpoint, videos_to_process, item_ids):
        """Generic copy process starter for both auto and manual modes."""
        destination_root = self.destination_path.get()
        if not destination_root or not os.path.isdir(destination_root):
            messagebox.showerror("Lỗi", "Vui lòng chọn một thư mục đích hợp lệ.")
            return
        
        # --- Disk Space Check ---
        files_to_copy_paths = [v['path'] for v in videos_to_process]
        required_space = file_operations.get_required_space(files_to_copy_paths)
        
        drive_name = get_drive_name_from_mountpoint(mountpoint)
        final_destination = os.path.join(destination_root, drive_name)
        
        if not file_operations.has_enough_space(destination_root, required_space):
            messagebox.showerror("Thiếu Dung Lượng", 
                                 "Không đủ dung lượng trống tại '{}'.\nCần: {:.2f} GB\nCòn lại: {:.2f} GB".format(
                                     destination_root, 
                                     required_space / (1024**3), 
                                     psutil.disk_usage(destination_root).free / (1024**3)))
            return

        try:
            os.makedirs(final_destination, exist_ok=True)
        except OSError as e:
            messagebox.showerror("Lỗi", "Không thể tạo thư mục đích:\n{}".format(e))
            return
        
        if self.delete_after_copy.get():
            # For manual mode, confirm with the number of files from selected drives.
            # For auto mode, confirm with the drive name.
            confirm_message = ""
            if mountpoint == "Manual":
                num_files = len(videos_to_process)
                confirm_message = f"Bạn có chắc chắn muốn XÓA VĨNH VIỄN {num_files} file đã chọn sau khi sao chép thành công không?"
            else:
                drive_name = get_drive_name_from_mountpoint(mountpoint)
                confirm_message = f"Bạn có chắc chắn muốn XÓA VĨNH VIỄN các file gốc trên thẻ {drive_name} sau khi sao chép thành công không?"

            if not messagebox.askyesno("Xác Nhận Xóa", confirm_message, icon='warning'):
                return

        # Show progress bar
        self.progress_frame.grid()
        self.progress_bar.set(0)
        self.progress_status_label.configure(text="Chuẩn bị sao chép...")
        self.log_message("Bắt đầu quá trình sao chép cho {}...".format(get_drive_name_from_mountpoint(mountpoint) if mountpoint != 'Manual' else 'các file đã chọn'))

        threading.Thread(target=self._copy_process_thread, 
                         args=(videos_to_process, destination_root, self.delete_after_copy.get(), item_ids, mountpoint), 
                         daemon=True).start()

    def _copy_process_thread(self, videos, destination_root, should_delete, item_ids, process_id):
        """The main worker thread for copying, verifying, and deleting."""
        final_results = {"success": 0, "error": 0, "skipped": 0}
        conflict_policy = self.conflict_policy.get()
        total_files = len(videos)
        
        is_manual_mode = (item_ids is not None)

        for i, video_info in enumerate(videos):
            source_path = video_info['path']
            file_name = os.path.basename(source_path)

            # Determine the correct subfolder (drive name)
            if is_manual_mode:
                # In manual mode, we get the drive mountpoint from the video_info dictionary
                # which was stored when the file list was created.
                drive_mountpoint = video_info.get('drive', 'Unknown_Drive')
                drive_name = get_drive_name_from_mountpoint(drive_mountpoint)
            else:
                # In auto mode, the process_id is the mountpoint
                drive_name = get_drive_name_from_mountpoint(process_id)

            final_destination_folder = os.path.join(destination_root, drive_name)
            try:
                os.makedirs(final_destination_folder, exist_ok=True)
            except OSError as e:
                self.after(0, self.log_message, f"Không thể tạo thư mục {final_destination_folder}: {e}", "ERROR")
                final_results["error"] += 1
                continue # Skip this file

            # Update overall progress
            progress_value = (i + 1) / total_files
            progress_text = f"Đang xử lý {i+1}/{total_files}: {file_name}"
            self.after(0, lambda p=progress_value: self.progress_bar.set(p))
            self.after(0, lambda t=progress_text: self.progress_status_label.configure(text=t))

            # Define status callback
            def status_callback(status_key, status_text):
                if is_manual_mode:
                    item_id = item_ids[i]
                    # Check if the item still exists in the treeview
                    if self.video_tree.exists(item_id):
                        self.after(0, self.update_item_status, item_id, status_text, status_key)
                self.after(0, self.log_message, f"{file_name}: {status_text}", level=status_key.upper())

            # Perform the core operation
            try:
                success, skipped = file_operations.copy_verify_delete_file(source_path, final_destination_folder, should_delete, conflict_policy, status_callback)
                if success:
                    final_results["success"] += 1
                    if skipped:
                        final_results["skipped"] += 1
                else:
                    final_results["error"] += 1
            except Exception as e:
                final_results["error"] += 1
                status_callback("error", f"Lỗi nghiêm trọng: {e}")
        
        # --- Finalize ---
        report_id = get_drive_name_from_mountpoint(process_id) if not is_manual_mode else "các file đã chọn"
        self.after(0, self.show_final_report, final_results, report_id)
        self.after(0, lambda: self.progress_frame.grid_remove()) # Hide progress bar
        self.after(0, self.log_message, f"Hoàn tất quá trình cho {report_id}.")
        self.after(0, self._update_ui_states)

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
                drive_name
            ))

            self.video_item_map[item_id] = { "path": video_data['path'], "drive": video_data['drive'] }
            # Schedule a UI update after adding an item
            self.after(50, self._update_ui_states)
            return item_id
        except Exception as e:
            print(f"Error adding video to list for {video_data.get('path', 'N/A')}: {e}")
            return None

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

    def update_item_status(self, item_id, status_text, tag):
        try:
            final_text = status_text
            if tag == 'success' and 'Hoàn thành' in status_text:
                final_text = f"✔ {status_text}"

            self.video_tree.set(item_id, "status", final_text)
            self.video_tree.item(item_id, tags=(tag,))
        except tk.TclError:
            print(f"Could not update status for item {item_id} (it may have been deleted).")
