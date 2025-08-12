import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import os

# Import from our new modules
import config
import file_operations
import drive_manager

class AutoCopierApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        # --- Window Setup ---
        self.title("Auto Video Copier")
        self.geometry("1100x800")
        self.configure(fg_color=config.COLOR_BG)
        ctk.set_appearance_mode("Dark")

        # --- App State Variables ---
        self.destination_path = ctk.StringVar()
        self.delete_after_copy = ctk.BooleanVar(value=False)
        self.is_auto_mode = ctk.BooleanVar(value=False)
        self.detected_drives = {}  # {mountpoint: {data}}
        self.video_item_map = {}   # {item_id: {data}}
        self.active_drive = None
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

    def on_closing(self):
        """Handle window close event."""
        self.monitoring = False
        self.destroy()

    def load_app_config(self):
        """Load configuration from file."""
        conf = config.load_config()
        self.destination_path.set(conf.get("destination_path", ""))

    def browse_destination(self):
        """Open browse dialog and save selected path."""
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.destination_path.set(folder_selected)
            config.save_config({"destination_path": self.destination_path.get()})

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
        
        ctk.CTkLabel(left_panel, text="Thiết Bị", font=ctk.CTkFont(size=18, weight="bold")).grid(row=0, column=0, columnspan=2, sticky='w', padx=15, pady=10)
        
        self.drive_list_frame = ctk.CTkScrollableFrame(left_panel, fg_color="transparent")
        self.drive_list_frame.grid(row=1, column=0, columnspan=2, sticky='nsew', padx=5)

        refresh_button = ctk.CTkButton(left_panel, text="Làm Mới", command=self.update_drive_list, fg_color=config.COLOR_ACCENT_TEAL)
        refresh_button.grid(row=2, column=0, sticky='ew', padx=(10, 5), pady=10)
        self.eject_button = ctk.CTkButton(left_panel, text="Tháo An Toàn", command=self.eject_active_drive, state="disabled")
        self.eject_button.grid(row=2, column=1, sticky='ew', padx=(5, 10), pady=10)

    def _create_right_panel(self):
        """Build the right panel for controls and video list."""
        right_panel = ctk.CTkFrame(self, fg_color="transparent")
        right_panel.grid(row=0, column=1, sticky='nsew', padx=(0, 15), pady=15)
        right_panel.grid_rowconfigure(1, weight=1)
        right_panel.grid_columnconfigure(0, weight=1)

        # --- Controls and Mode ---
        top_controls_frame = ctk.CTkFrame(right_panel, fg_color="transparent")
        top_controls_frame.grid(row=0, column=0, sticky='ew')
        top_controls_frame.grid_columnconfigure(0, weight=1)

        dest_frame = ctk.CTkFrame(top_controls_frame, fg_color=config.COLOR_FRAME, corner_radius=15)
        dest_frame.grid(row=0, column=0, sticky='ew')
        dest_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(dest_frame, text="Thư Mục Đích", font=ctk.CTkFont(size=14, weight="bold")).grid(row=0, column=0, columnspan=2, sticky='w', padx=15, pady=(10,5))
        dest_entry = ctk.CTkEntry(dest_frame, textvariable=self.destination_path, height=35, corner_radius=10, border_width=0)
        dest_entry.grid(row=1, column=0, sticky='ew', padx=15, pady=(0,10))
        browse_button = ctk.CTkButton(dest_frame, text="Chọn...", command=self.browse_destination, width=80, height=35, corner_radius=10)
        browse_button.grid(row=1, column=1, sticky='e', padx=15, pady=(0,10))

        mode_frame = ctk.CTkFrame(top_controls_frame, fg_color=config.COLOR_FRAME, corner_radius=15)
        mode_frame.grid(row=0, column=1, sticky='ns', padx=(10,0))
        ctk.CTkLabel(mode_frame, text="Chế Độ", font=ctk.CTkFont(size=14, weight="bold")).pack(padx=15, pady=(10,5), anchor='w')
        mode_switch = ctk.CTkSwitch(mode_frame, text="Tự Động", variable=self.is_auto_mode, progress_color=config.COLOR_ACCENT_TEAL, command=self.toggle_auto_mode)
        mode_switch.pack(padx=15, pady=(0,10), anchor='w')

        # --- Video List ---
        video_frame = ctk.CTkFrame(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15)
        video_frame.grid(row=1, column=0, sticky='nsew', pady=10)
        video_frame.grid_rowconfigure(0, weight=1)
        video_frame.grid_columnconfigure(0, weight=1)
        self.video_tree = ttk.Treeview(video_frame, columns=("status", "name", "size", "modified"), show="headings", selectmode="extended")
        self.video_tree.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)

        # --- Action Buttons ---
        action_frame = ctk.CTkFrame(right_panel, fg_color=config.COLOR_FRAME, corner_radius=15)
        action_frame.grid(row=2, column=0, sticky='ew')
        action_frame.grid_columnconfigure(3, weight=1)
        self.select_all_button = ctk.CTkButton(action_frame, text="Chọn Tất Cả", command=self.select_all_videos, state="disabled")
        self.select_all_button.grid(row=0, column=0, padx=10, pady=10)
        self.deselect_all_button = ctk.CTkButton(action_frame, text="Bỏ Chọn Tất Cả", command=self.deselect_all_videos, state="disabled")
        self.deselect_all_button.grid(row=0, column=1, padx=5, pady=10)
        self.delete_checkbox = ctk.CTkCheckBox(action_frame, text="Tự động xóa file gốc", variable=self.delete_after_copy, fg_color=config.COLOR_ACCENT_TEAL)
        self.delete_checkbox.grid(row=0, column=2, padx=20, pady=10)
        self.copy_button = ctk.CTkButton(action_frame, text="Sao Chép Thủ Công", command=self.start_manual_copy, state="disabled", fg_color=config.COLOR_ACCENT_TEAL, font=ctk.CTkFont(weight="bold"))
        self.copy_button.grid(row=0, column=4, sticky='e', padx=10, pady=10)

    def setup_treeview_style(self):
        """Configure the style for the Treeview widget."""
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background=config.COLOR_FRAME, foreground=config.COLOR_TEXT, rowheight=28, fieldbackground=config.COLOR_FRAME, borderwidth=0, relief="flat")
        style.map('Treeview', background=[('selected', config.COLOR_ACCENT_TEAL)])
        style.configure("Treeview.Heading", font=('Arial', 13, 'bold'), background="#313335", foreground=config.COLOR_TEXT_HEADER, relief="flat")
        style.map("Treeview.Heading", background=[('active', '#3a3c3e')])
        self.video_tree.heading("status", text="Trạng Thái")
        self.video_tree.heading("name", text="Tên Tệp")
        self.video_tree.heading("size", text="Kích Thước")
        self.video_tree.heading("modified", text="Ngày Sửa Đổi")
        self.video_tree.column("status", width=120, anchor=tk.CENTER)
        self.video_tree.column("name", width=400, anchor=tk.W)
        self.video_tree.column("size", width=120, anchor=tk.E)
        self.video_tree.column("modified", width=180, anchor=tk.CENTER)
        self.video_tree.tag_configure('pending', foreground=config.COLOR_TEXT)
        self.video_tree.tag_configure('processing', foreground=config.COLOR_STATUS_WARN)
        self.video_tree.tag_configure('success', foreground=config.COLOR_STATUS_SUCCESS)
        self.video_tree.tag_configure('error', foreground=config.COLOR_STATUS_ERROR)

    # --- Core Logic and Event Handlers ---

    def toggle_auto_mode(self):
        """Handle the auto/manual mode switch."""
        is_auto = self.is_auto_mode.get()
        manual_controls_state = "disabled" if is_auto else "normal"
        if is_auto:
            self.copy_button.configure(state="disabled")
            self.select_all_button.configure(state="disabled")
            self.deselect_all_button.configure(state="disabled")
            messagebox.showinfo("Chế Độ Tự Động", "Đã bật chế độ tự động.")
        else:
            if self.active_drive:
                self.copy_button.configure(state="normal")
                self.select_all_button.configure(state="normal")
                self.deselect_all_button.configure(state="normal")
            messagebox.showinfo("Chế Độ Thủ Công", "Đã chuyển sang chế độ thủ công.")
            
    def monitor_drives_thread(self):
        """Background thread to monitor drive connections."""
        known_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
        while self.monitoring:
            try:
                current_mountpoints = {p.mountpoint for p in drive_manager.get_removable_drives()}
                new_drives = current_mountpoints - known_mountpoints
                removed_drives = known_mountpoints - current_mountpoints

                if new_drives or removed_drives:
                    self.after(0, self.update_drive_list)
                    if self.is_auto_mode.get() and new_drives:
                        for drive_mp in new_drives:
                            self.after(0, self.start_auto_process, drive_mp)
                
                known_mountpoints = current_mountpoints
            except Exception as e:
                print("Error in monitor thread: {}".format(e))
            time.sleep(3)

    def update_drive_list(self):
        """Refresh the list of drives in the left panel."""
        for widget in self.drive_list_frame.winfo_children():
            widget.destroy()
        
        current_drives = drive_manager.get_removable_drives()
        self.detected_drives.clear()

        for p in current_drives:
            drive_name = os.path.basename(p.mountpoint)
            drive_frame = ctk.CTkFrame(self.drive_list_frame, fg_color=config.COLOR_BG, corner_radius=10)
            drive_frame.pack(fill='x', expand=True, padx=5, pady=5)
            button = ctk.CTkButton(drive_frame, text=drive_name, command=lambda mp=p.mountpoint: self.select_drive(mp))
            button.pack(fill='x', expand=True)
            self.detected_drives[p.mountpoint] = {"button": button, "device": p.device, "processing": False}

    def select_drive(self, mountpoint):
        """Handle drive selection from the left panel."""
        self.active_drive = mountpoint
        self.eject_button.configure(state="normal")
        for mp, data in self.detected_drives.items():
            data["button"].configure(fg_color=config.COLOR_ACCENT_TEAL if mp == mountpoint else ctk.ThemeManager.theme["CTkButton"]["fg_color"])
        
        self.clear_video_list()
        self.list_videos_from_drive(mountpoint)

    def eject_active_drive(self):
        """Eject the currently selected drive."""
        if not self.active_drive or self.detected_drives.get(self.active_drive, {}).get("processing"):
            messagebox.showwarning("Cảnh Báo", "Không thể tháo thẻ nhớ đang trong quá trình xử lý hoặc chưa được chọn.")
            return

        device = self.detected_drives[self.active_drive]["device"]
        success, message = drive_manager.eject_drive(device)
        
        if success:
            messagebox.showinfo("Thành Công", message)
        else:
            messagebox.showerror("Lỗi", message)
        self.update_drive_list()

    def list_videos_from_drive(self, mountpoint):
        """Start a thread to find and list videos from a drive."""
        self.clear_video_list()
        if not self.is_auto_mode.get():
            self.select_all_button.configure(state="normal")
            self.deselect_all_button.configure(state="normal")
            self.copy_button.configure(state="normal")
        threading.Thread(target=self._search_videos_thread, args=(mountpoint,), daemon=True).start()

    def _search_videos_thread(self, drive_path):
        """Worker thread to find videos."""
        for video_data in file_operations.find_videos_on_drive(drive_path):
            self.after(0, self.add_video_to_list, video_data)

    def start_manual_copy(self):
        """Initiate the copy process for manually selected files."""
        if not self.active_drive:
            messagebox.showwarning("Cảnh Báo", "Vui lòng chọn một thẻ nhớ.")
            return
        selected_item_ids = self.video_tree.selection()
        if not selected_item_ids:
            messagebox.showwarning("Cảnh Báo", "Vui lòng chọn ít nhất một video để sao chép.")
            return
        videos_to_process = {item_id: self.video_item_map[item_id] for item_id in selected_item_ids}
        self.start_copy_process(self.active_drive, videos_to_process)

    def start_auto_process(self, mountpoint):
        """Fully automated workflow for a newly detected drive."""
        if not self.is_auto_mode.get(): return
        if self.detected_drives.get(mountpoint):
            self.detected_drives[mountpoint]["processing"] = True
            self.after(0, lambda: self.detected_drives[mountpoint]["button"].configure(state="disabled", text="{} (Đang xử lý...)".format(os.path.basename(mountpoint))))
        threading.Thread(target=self._auto_process_thread, args=(mountpoint,), daemon=True).start()

    def _auto_process_thread(self, mountpoint):
        """Worker thread for auto-processing."""
        all_videos_on_drive = list(file_operations.find_videos_on_drive(mountpoint))
        if not all_videos_on_drive:
            print("Không tìm thấy video nào trên {}.".format(mountpoint))
            if self.detected_drives.get(mountpoint):
                self.detected_drives[mountpoint]["processing"] = False
                self.after(0, lambda: self.detected_drives[mountpoint]["button"].configure(state="normal", text=os.path.basename(mountpoint)))
            return
        
        # Create a video map similar to the manual one
        video_map = {f"auto_item_{i}": {'path': path, 'status': 'pending'} for i, (_, _, _, path) in enumerate(all_videos_on_drive)}
        self.after(0, self.start_copy_process, mountpoint, video_map)
    
    def start_copy_process(self, mountpoint, videos_to_process):
        """Generic copy process starter for both auto and manual modes."""
        destination_root = self.destination_path.get()
        if not destination_root or not os.path.isdir(destination_root):
            messagebox.showerror("Lỗi", "Vui lòng chọn một thư mục đích hợp lệ.")
            return
        
        drive_name = os.path.basename(mountpoint)
        final_destination = os.path.join(destination_root, drive_name)
        try:
            os.makedirs(final_destination, exist_ok=True)
        except OSError as e:
            messagebox.showerror("Lỗi", "Không thể tạo thư mục đích:\n{}".format(e))
            return
        
        if self.delete_after_copy.get():
            if not messagebox.askyesno("Xác Nhận Xóa", "Bạn có chắc chắn muốn XÓA VĨNH VIỄN các file gốc trên thẻ {} sau khi sao chép thành công không?".format(drive_name), icon='warning'):
                return

        threading.Thread(target=self._copy_process_thread, args=(mountpoint, videos_to_process, final_destination, self.delete_after_copy.get()), daemon=True).start()

    def _copy_process_thread(self, mountpoint, videos, destination, should_delete):
        """The main worker thread for copying, verifying, and deleting."""
        self.detected_drives[mountpoint]["processing"] = True
        self.after(0, lambda: self.eject_button.configure(state="disabled"))
        
        final_results = {"success": 0, "error": 0}
        is_active_drive = (mountpoint == self.active_drive)

        for item_id, video_info in videos.items():
            real_item_id = item_id
            if self.is_auto_mode.get() and is_active_drive:
                # Add video to list only if the processed drive is the one currently viewed
                video_data = next((v for v in file_operations.find_videos_on_drive(mountpoint) if v[3] == video_info['path']), None)
                if video_data:
                    real_item_id = self.add_video_to_list(video_data)
                    self.update()

            def status_callback(status_key, status_text):
                if is_active_drive:
                    self.after(0, self.update_item_status, real_item_id, status_text, status_key)

            success = file_operations.copy_verify_delete_file(video_info['path'], destination, should_delete, status_callback)
            if success:
                final_results["success"] += 1
            else:
                final_results["error"] += 1

        if self.detected_drives.get(mountpoint):
            self.detected_drives[mountpoint]["processing"] = False
            self.after(0, lambda: self.detected_drives[mountpoint]["button"].configure(state="normal", fg_color=config.COLOR_STATUS_SUCCESS, text="{} (Hoàn thành)".format(os.path.basename(mountpoint))))

        self.after(0, lambda: self.eject_button.configure(state="normal" if self.active_drive == mountpoint else "disabled"))
        self.after(0, self.show_final_report, final_results, os.path.basename(mountpoint))

    # --- UI Helpers ---

    def add_video_to_list(self, video_data):
        """Add a video entry to the Treeview and returns the item ID."""
        file_name, file_size, mod_time, full_path = video_data
        item_id = self.video_tree.insert("", "end", values=("Chờ", file_name, file_size, mod_time), tags=('pending',))
        self.video_item_map[item_id] = {'path': full_path, 'status': 'pending'}
        return item_id
        
    def show_final_report(self, results, drive_name):
        """Display a summary report after a process finishes."""
        message = "Hoàn tất xử lý cho thẻ nhớ: {}\n\nThành công: {} tệp\nThất bại: {} tệp".format(drive_name, results['success'], results['error'])
        if results["error"] > 0:
            messagebox.showwarning("Báo Cáo", message)
        else:
            messagebox.showinfo("Báo Cáo", message)

    def select_all_videos(self): self.video_tree.selection_add(*self.video_tree.get_children())
    def deselect_all_videos(self): self.video_tree.selection_remove(*self.video_tree.get_children())
    def clear_video_list(self):
        self.video_tree.delete(*self.video_tree.get_children())
        self.video_item_map.clear()
        if not self.is_auto_mode.get():
            self.copy_button.configure(state="disabled")
            self.select_all_button.configure(state="disabled")
            self.deselect_all_button.configure(state="disabled")
    def update_item_status(self, item_id, status_text, tag):
        self.video_tree.set(item_id, "status", status_text)
        self.video_tree.item(item_id, tags=(tag,))
