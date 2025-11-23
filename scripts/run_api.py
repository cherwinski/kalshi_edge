"""Run the FastAPI app via uvicorn."""
from __future__ import annotations

import uvicorn


def main() -> None:
    uvicorn.run("kalshi_edge.api.app:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
