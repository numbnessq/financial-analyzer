import subprocess
import sys
import os
import threading
import time
import urllib.request
import urllib.error

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(root)


def kill_port(port):
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True, text=True
        )
        for pid in result.stdout.strip().split("\n"):
            if pid:
                subprocess.run(["kill", "-9", pid])
    except Exception:
        pass


kill_port(8000)
kill_port(3000)


# ── Frontend сервер на порту 3000 ──────────────────────────────────────────
def start_frontend():
    subprocess.run(
        [sys.executable, "-m", "http.server", "3000",
         "--bind", "127.0.0.1"],
        cwd=os.path.join(root, "frontend")
    )


threading.Thread(target=start_frontend, daemon=True).start()


# ── Backend на порту 8000 ──────────────────────────────────────────────────
proc = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "backend.main:app",
     "--host", "127.0.0.1", "--port", "8000"],
    cwd=root,
)

# Ждём пока backend реально поднимется (до 30 сек)
for _ in range(30):
    try:
        urllib.request.urlopen("http://127.0.0.1:8000/ping", timeout=1)
        break
    except Exception:
        time.sleep(1)

proc.wait()