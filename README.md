Python client for the BV-BRC Solr API.

Requirements
------------
- Python >= 3.10
- pip (and optionally `venv`)

Install from source (editable)
------------------------------
1) Clone this monorepo:

```bash
git clone <YOUR_GIT_REMOTE_URL>/BVBRC-MCP-Servers.git
cd BVBRC-MCP-Servers/bvbrc-solr-python-api
```

2) (Optional) Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3) Install the package in editable mode:

```bash
pip install -U pip
pip install -e .
```

Notes
-----
- The package depends on `httpx>=0.28` and targets Python 3.10+.
- See `pyproject.toml` for metadata and dependencies.


