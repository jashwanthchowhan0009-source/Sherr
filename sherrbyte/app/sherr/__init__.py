"""Sherr-I generation layer.

Deliberately empty. This package used to do `from . import admin`, and that one
line made every `app.sherr.*` import fail: admin.py was a copy of main.py's
sqlite /admin/relink and /admin/rescope handlers, dropped into the Postgres
engine, importing get_db / link_stories / classify_scope from app.sherr.core —
none of which core.py defines or ever did. So `from app.sherr import router`,
`watermark`, `writer`, `rag` and `core` all raised ImportError, taking the whole
generation layer and app/main.py down with them.

It also formed a cycle: app.sherr -> admin -> core -> `from app.sherr import
rag, watermark, writer`. Keeping this module free of imports is what stops that
from reappearing; submodules are imported by path, not re-exported here.
"""
