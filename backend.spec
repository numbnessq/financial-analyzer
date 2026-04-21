# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['/Users/dmitrij/PyCharmMiscProject/project/backend/main.py'],
    pathex=['/Users/dmitrij/PyCharmMiscProject/project'],
    binaries=[],
    datas=[],
    hiddenimports=['fastapi', 'fastapi.middleware.cors', 'uvicorn', 'uvicorn.logging', 'uvicorn.loops', 'uvicorn.loops.auto', 'uvicorn.protocols', 'uvicorn.protocols.http', 'uvicorn.protocols.http.auto', 'uvicorn.protocols.http.h11_impl', 'uvicorn.protocols.websockets.auto', 'uvicorn.lifespan.on', 'starlette', 'starlette.routing', 'pydantic', 'anyio', 'anyio.abc', 'anyio._backends._asyncio', 'backend.pipeline.parser', 'backend.pipeline.ai_extractor', 'backend.pipeline.normalizer', 'backend.pipeline.source_mapper', 'backend.pipeline.matcher', 'backend.pipeline.analyzer', 'backend.pipeline.scorer', 'backend.pipeline.explainer', 'backend.pipeline.graph_builder'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
