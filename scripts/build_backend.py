#!/usr/bin/env python3
import subprocess, sys, platform, shutil, os

TRIPLE_MAP = {
    ("darwin", "arm64"):  "aarch64-apple-darwin",
    ("darwin", "x86_64"): "x86_64-apple-darwin",
    ("win32",  "amd64"):  "x86_64-pc-windows-msvc",
    ("win32",  "x86_64"): "x86_64-pc-windows-msvc",
    ("linux",  "x86_64"): "x86_64-unknown-linux-gnu",
    ("linux",  "aarch64"):"aarch64-unknown-linux-gnu",
}

def get_triple():
    s = sys.platform.lower()
    m = platform.machine().lower()
    t = TRIPLE_MAP.get((s, m))
    if not t:
        raise RuntimeError(f"Unknown platform: {s}/{m}")
    return t

def main():
    triple = get_triple()
    ext    = ".exe" if sys.platform == "win32" else ""

    # Пути относительно tauri/ (откуда запускает Tauri CLI)
    out_dir  = os.path.join("src-tauri", "binaries")
    os.makedirs(out_dir, exist_ok=True)

    # backend/main.py — относительно корня проекта, но скрипт запускается из tauri/
    backend_entry = os.path.join("..", "backend", "main.py")

    subprocess.run([
        sys.executable, "-m", "PyInstaller",
        "--onefile", "--clean",
        "--name", "backend",
        "--distpath", os.path.join("src-tauri", "binaries", "_tmp"),
        # Скрытые импорты для uvicorn
        "--hidden-import=uvicorn.logging",
        "--hidden-import=uvicorn.loops",
        "--hidden-import=uvicorn.loops.auto",
        "--hidden-import=uvicorn.protocols.http.auto",
        "--hidden-import=uvicorn.protocols.websockets.auto",
        "--hidden-import=uvicorn.lifespan.on",
        backend_entry
    ], check=True)

    src  = os.path.join("src-tauri", "binaries", "_tmp", f"backend{ext}")
    dest = os.path.join(out_dir, f"backend-{triple}{ext}")
    shutil.copy2(src, dest)
    if sys.platform != "win32":
        os.chmod(dest, 0o755)
    shutil.rmtree(os.path.join("src-tauri", "binaries", "_tmp"), ignore_errors=True)

    print(f"[build_backend] OK: {dest}")


if __name__ == "__main__":
    main()