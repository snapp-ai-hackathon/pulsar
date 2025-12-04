from __future__ import annotations

try:
    from pulsar.cli import main
except (
    ModuleNotFoundError
) as exc:  # pragma: no cover - convenience for direct script usage
    raise ModuleNotFoundError(
        "Unable to import the Pulsar CLI. Install the project first, e.g. `uv sync` "
        "followed by `uv run pulsar ...`."
    ) from exc

__all__ = ["main"]


if __name__ == "__main__":
    main()
