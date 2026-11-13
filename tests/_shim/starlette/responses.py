"""Minimal ``starlette.responses`` shim mirroring the package layout."""
from starlette import JSONResponse, Response

__all__ = ["JSONResponse", "Response"]
