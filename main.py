from app import AutoCopierApp
import sys
from PySide6.QtWidgets import QApplication

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AutoCopierApp()
    window.show()
    sys.exit(app.exec())
