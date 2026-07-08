# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

The repository contains a single module, `k8s_watch` — an async Kubernetes resource
watcher (`WatchResource`) with an offline (respx-mocked) test suite plus one live test
that requires a reachable cluster.

## Tooling

- Python >= 3.13, managed with [uv](https://docs.astral.sh/uv/).
- Add a dependency: `uv add <package>`
- Add a dev-only dependency: `uv add --dev <package>`
- Sync the environment from `pyproject.toml`: `uv sync`
- Run the offline test suite: `uv run pytest`
- Use `uvx ruff check`, `uvx ruff format` and `uvx ty check` for linting.

## Conventions

- **Error handling:** functions do not raise exceptions. Instead they return an
  error boolean, eg `tuple[result, bool]`. The trailing `bool` is an error flag
  (`True` == error). Check it.
- **Code Philosophy**: If it's not tested it's broken.
- **Coverage**:  must remain at 100%.
