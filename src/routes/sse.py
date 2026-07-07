from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from xcore.kernel.api.rbac import get_current_user, require_permission
from xcore.sdk import AuthPayload, error, get_logger, ok

from ..utils import parse_channels, require_pubsub

if TYPE_CHECKING:
    from extensions.pubsub.service import PubSubClient

logger = get_logger("xpulses")


def _extract_tenant_id(request: Request) -> str | None:
    if hasattr(request.state, "user") and request.state.user:
        return request.state.user.get("tenant_id") or request.state.user.get(
            "user", {}
        ).get("tenant_id")

    tenant_id = request.headers.get("X-Tenant-Id")
    if tenant_id:
        return tenant_id

    auth = (
        request.headers.get("Authorization")
        or request.headers.get("authorization")
        or ""
    )
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        if token:
            try:
                from jose import jwt as jose_jwt
                claims = jose_jwt.get_unverified_claims(token)
                return claims.get("tenant_id")
            except Exception:
                raise HTTPException(status_code=401, detail="Invalid token")
    return None


def sse_routes(svc: Any, caller: Any = None) -> APIRouter:

    router = APIRouter(tags=["xpulse"])

    @router.get("/stream")
    async def stream_response(
        user: AuthPayload = Depends(get_current_user),
        channels: list[str] = Query(
            default=["notification"], description="channel to listen system msg"
        ),
    ):
        _svc = require_pubsub(svc)
        _channels = parse_channels(channels)

        pending = await _svc.flush_inbox(user["sub"])

        async def stream_with_inbox():
            import json as _json

            for msg in pending:
                ch = msg.get("channel", "notification")
                yield (f"event: {ch}\ndata: {_json.dumps(msg, ensure_ascii=False)}\n\n")
            async for chunk in _svc.stream(channels=_channels, user_id=user["sub"]):
                yield chunk

        return StreamingResponse(
            stream_with_inbox(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "redis",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @router.get("/inbox", tags=["xpulse"])
    async def get_inbox(current_user: AuthPayload = Depends(get_current_user)):
        redis = require_pubsub(svc)
        user_id: str = current_user.get("sub", "")
        messages = await redis.flush_inbox(user_id)
        return {"messages": messages, "count": len(messages)}

    @router.get("/inbox/count", tags=["xpulse"])
    async def inbox_count(current_user: AuthPayload = Depends(get_current_user)):
        redis = require_pubsub(svc)
        user_id: str = current_user.get("sub", "")
        count = await redis.inbox_count(user_id)
        return ok(count=count)

    @router.post("/publish", tags=["xpulse"])
    async def publish(
        user_id: str = Query(..., description="ID de l'utilisateur cible"),
        text: str = Query(..., description="Message à envoyer"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:admin:publish")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(channels)

        results = await redis.publish_many(
            parsed_channels, {"user_id": user_id, "text": text}
        )
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Publish échoué sur : {failed}",
            )
        return ok(channels=parsed_channels)

    @router.post("/tenant/publish", tags=["xpulse"])
    async def publish_tenant(
        request: Request,
        user_id: str = Query(..., description="ID de l'utilisateur cible"),
        text: str = Query(..., description="Message à envoyer"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:tenant:publish")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(channels)

        if caller is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="IPC caller non disponible.",
            )
        tenant_id = _extract_tenant_id(request)
        users = (
            await caller("auth", "xauth.users.list.tenant", {"tenant_id": tenant_id})
        ).get("users", [])
        if user_id not in users:
            logger.warning(f"User {user_id} {users} {tenant_id}")
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail="User not in your organization.",
            )

        results = await redis.publish_many(
            parsed_channels, {"user_id": user_id, "text": text}
        )
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Publish échoué sur : {failed}",
            )
        return ok(channels=parsed_channels)

    @router.post("/broadcast", tags=["xpulse"])
    async def broadcast(
        text: str = Query(..., description="Message à broadcaster"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:admin:broadcast")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(channels)

        results = await redis.publish_many(parsed_channels, {"text": text})
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {failed}",
            )
        logger.info("Broadcast : channels=%s", parsed_channels)
        return ok(channels=parsed_channels)

    @router.post("/tenant/broadcast", tags=["xpulse"])
    async def tenant_broadcast(
        request: Request,
        text: str = Query(..., description="Message à broadcaster"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:tenant:broadcast")),
    ):
        redis = require_pubsub(svc)
        tenant_id = _extract_tenant_id(request)
        channel = f"tenant-{tenant_id}"

        response = await redis.publish(channel, {"text": text})
        if not response:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {channel}",
            )

        return ok(channels=[channel])

    return router
