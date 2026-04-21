#!/usr/bin/env python3
import subprocess, sys, platform, shutil, os

TRIPLE_MAP = {
    ("darwin", "arm64"):  "aarch64-apple-darwin",
    ("darwin", "x86_64"): "x86_64-apple-darwin",
    ("win32",  "amd64"):  "x86_64-pc-windows-msvc",
    ("win32",  "x86_64"): "x86_64-pc-windows-msvc",
    ("linux",  "x86_64"): "x86_64-unknown-linux-gnu",
    ("linux",  "aarch64"): "aarch64-unknown-linux-gnu",
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

    out_dir = os.path.join("src-tauri", "binaries")
    os.makedirs(out_dir, exist_ok=True)

    # Корень проекта (на уровень выше tauri/)
    project_root  = os.path.abspath(os.path.join("."))
    backend_entry = os.path.join(project_root, "backend", "main.py")

    subprocess.run([
        sys.executable, "-m", "PyInstaller",
        "--onefile", "--clean",
        "--name", "backend",
        "--distpath", os.path.join("src-tauri", "binaries", "_tmp"),
        "--paths", project_root,
        "--hidden-import=fastapi",
        "--hidden-import=fastapi.middleware.cors",
        "--hidden-import=uvicorn",
        "--hidden-import=uvicorn.logging",
        "--hidden-import=uvicorn.loops",
        "--hidden-import=uvicorn.loops.auto",
        "--hidden-import=uvicorn.protocols",
        "--hidden-import=uvicorn.protocols.http",
        "--hidden-import=uvicorn.protocols.http.auto",
        "--hidden-import=uvicorn.protocols.http.h11_impl",
        "--hidden-import=uvicorn.protocols.websockets.auto",
        "--hidden-import=uvicorn.lifespan.on",
        "--hidden-import=starlette",
        "--hidden-import=starlette.routing",
        "--hidden-import=pydantic",
        "--hidden-import=anyio",
        "--hidden-import=anyio.abc",
        "--hidden-import=anyio._backends._asyncio",
        "--hidden-import=backend.pipeline.parser",
        "--hidden-import=backend.pipeline.ai_extractor",
        "--hidden-import=backend.pipeline.normalizer",
        "--hidden-import=backend.pipeline.source_mapper",
        "--hidden-import=backend.pipeline.matcher",
        "--hidden-import=backend.pipeline.analyzer",
        "--hidden-import=backend.pipeline.scorer",
        "--hidden-import=backend.pipeline.explainer",
        "--hidden-import=backend.pipeline.graph_builder",
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