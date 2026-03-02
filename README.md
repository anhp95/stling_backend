# Research Platform Backend

Developed with Python and managed using [uv](https://github.com/astral-sh/uv).

## Setup

1. Install `uv` if you haven't already.
2. The environment will be automatically created when running commands with `uv run`.

## Running the Server

To start the FastAPI server with auto-reload:

```bash
uv run python main.py
```

Or using uvicorn directly:

```bash
uv run uvicorn main:app --reload
```

## Adding Dependencies

To add a new package:

```bash
uv add <package-name>
```

## Managing Python Version

The Python version is specified in `.python-version`. To use a different version:

```bash
uv python pin 3.12
```
