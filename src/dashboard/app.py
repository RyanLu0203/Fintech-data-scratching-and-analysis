"""Backward-compatible Streamlit entry point.

Use ``streamlit run src/dashboard/streamlit_app.py`` for the full dashboard.
This module remains so older commands that reference ``src/dashboard/app.py``
still launch the same dashboard.
"""

from src.dashboard.streamlit_app import *  # noqa: F401,F403
