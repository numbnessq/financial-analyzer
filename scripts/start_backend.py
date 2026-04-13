# scripts/start_backend.py
# Запускается из Tauri при старте приложения

import subprocess
import sys
import os

# Корень проекта — на уровень выше scripts/
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(root)

subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "backend.main:app",
     "--host", "127.0.0.1", "--port", "8000"],
    cwd=root,
)