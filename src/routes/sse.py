from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from xcore.kernel.api.rbac import get_current_user, require_permission
from xcore.sdk import AuthPayload, error, get_logger, ok

from ..utils import authorize_channels, is_broadcast_channel, parse_channels, require_pubsub

if TYPE_CHECKING:
    from extensions.pubsub.service import PubSubClient

logger = get_logger("xpulses")


# `user_id`/`text` en Body plutôt qu'en Query : une query string se retrouve
# typiquement dans les logs d'accès serveur/proxy et l'historique
# navigateur — un mauvais choix pour un identifiant utilisateur et un
# contenu de notification pouvant porter des données personnelles (audit
# XPulse Constat 7).
class PublishBody(BaseModel):
    user_id: str = Field(..., description="ID de l'utilisateur cible")
    text: str = Field(..., description="Message à envoyer")
    channels: list[str] = Field(default=["notification"])


class BroadcastBody(BaseModel):
    text: str = Field(..., description="Message à broadcaster")
    channels: list[str] = Field(default=["notification"])


def _extract_tenant_id(request: Request) -> str | None:
    # Les deux seuls appelants (`publish_tenant`, `tenant_broadcast`)
    # dépendent de `Depends(require_permission(...))`, qui pose
    # `request.state.user` (vérifié RS256) avant l'exécution du corps de la
    # route — le repli header `X-Tenant-Id` et le décodage JWT non vérifié
    # étaient inatteignables en pratique, et l'import `jose` n'était même pas
    # déclaré dans `allowed_imports`/`requirements.txt` (audit XPulse
    # Constat 4). Supprimés.
    if hasattr(request.state, "user") and request.state.user:
        return request.state.user.get("tenant_id") or request.state.user.get(
            "user", {}
        ).get("tenant_id")
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
        authorize_channels(_channels, user)
        _unfiltered = {ch for ch in _channels if is_broadcast_channel(ch)}

        pending = await _svc.flush_inbox(user["sub"])

        async def stream_with_inbox():
            import json as _json

            for msg in pending:
                ch = msg.get("channel", "notification")
                yield (f"event: {ch}\ndata: {_json.dumps(msg, ensure_ascii=False)}\n\n")
            async for chunk in _svc.stream(
                channels=_channels, user_id=user["sub"], unfiltered_channels=_unfiltered
            ):
                yield chunk

        return StreamingResponse(
            stream_with_inbox(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
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
        body: PublishBody,
        _: AuthPayload = Depends(require_permission("xpulse:admin:publish")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(body.channels)

        results = await redis.publish_many(
            parsed_channels, {"user_id": body.user_id, "text": html.escape(body.text)}
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
        body: PublishBody,
        _: AuthPayload = Depends(require_permission("xpulse:tenant:publish")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(body.channels)

        if caller is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="IPC caller non disponible.",
            )
        tenant_id = _extract_tenant_id(request)
        users = (
            await caller("auth", "xauth.users.list.tenant", {"tenant_id": tenant_id})
        ).get("users", [])
        if body.user_id not in users:
            # Pas de liste d'utilisateurs (PII : emails) en clair dans les
            # logs — seul l'identifiant refusé et le tenant sont utiles au
            # diagnostic (audit XPulse, dette technique « Logging de PII »).
            logger.warning("Publish tenant refusé : user_id=%s tenant_id=%s", body.user_id, tenant_id)
            # 403 (refus d'autorisation), pas 406 (négociation de contenu —
            # sémantiquement incorrect ici, audit XPulse dette technique).
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User not in your organization.",
            )

        results = await redis.publish_many(
            parsed_channels, {"user_id": body.user_id, "text": html.escape(body.text)}
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
        body: BroadcastBody,
        _: AuthPayload = Depends(require_permission("xpulse:admin:broadcast")),
    ):
        redis = require_pubsub(svc)
        parsed_channels = parse_channels(body.channels)

        results = await redis.publish_many(parsed_channels, {"text": html.escape(body.text)})
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
        body: BroadcastBody,
        _: AuthPayload = Depends(require_permission("xpulse:tenant:broadcast")),
    ):
        redis = require_pubsub(svc)
        tenant_id = _extract_tenant_id(request)
        channel = f"tenant-{tenant_id}"

        response = await redis.publish(channel, {"text": html.escape(body.text)})
        if not response:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {channel}",
            )

        return ok(channels=[channel])

    return router
