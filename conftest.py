"""
Root conftest.py — automatically adds all necessary paths to sys.path
so that pytest can find the shared module and service modules regardless
of which directory pytest is run from.
"""

import sys
import os

# Get the absolute path of the project root
project_root = os.path.dirname(os.path.abspath(__file__))

# Add paths that mirror the PYTHONPATH set in each service's Dockerfile
paths_to_add = [
    project_root,                                    # for shared/
    os.path.join(project_root, "api_gateway"),       # for auth, rate_limiter
    os.path.join(project_root, "inference_gateway"), # for router, sse_manager
    os.path.join(project_root, "worker"),            # for engine, publisher
    os.path.join(project_root, "control_plane"),     # for tenant_store
]

for path in paths_to_add:
    if path not in sys.path:
        sys.path.insert(0, path)