from .sse import sse_routes
from fastapi import APIRouter
from typing import Any





def builder_router(svc:Any, caller:Any) -> APIRouter:

    router = APIRouter(tags=["xpulse"])
    router.include_router(sse_routes(serice=svc, caller=caller))

    return router


__all__ = [
    "sse_routes",
    "builder_router"
]