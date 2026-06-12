from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from xcore.kernel.api.rbac import get_current_user, require_permission
from xcore.sdk import AuthPayload, error, get_logger, ok

from ..client import InvalidChannel, validate_channels

if TYPE_CHECKING:
    from ..client.redis_client import RedisPubSubManager

logger = get_logger("xpulses")


def _extract_tenant_id(request: Request) -> str | None:
    """
    Cherche le tenant_id dans :
        1. request.state (si XAuthBackend a déjà décodé le token)
        2. Header X-Tenant-Id (explicite)
        3. JWT Authorization — decode WITHOUT vérification pour extraire tenant_id
    """
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


def _parse_channels(raw: list[str]) -> list[str]:
    flat = []
    for c in raw:
        flat.extend(c.split(","))
    try:
        return validate_channels(flat)
    except InvalidChannel as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))


def _require_redis(svc: "RedisPubSubManager") -> "RedisPubSubManager":
    if not svc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service Redis indisponible.",
        )
    return svc


def sse_routes(serice: "RedisPubSubManager", caller: Any = None) -> "APIRouter":

    router = APIRouter(tags=["xpulse"])

    # stream de reponse et gestion d'abonnement contrat du frontend
    @router.get("/stream")
    async def stream_response(
        user: AuthPayload = Depends(get_current_user),
        channels: list[str] = Query(
            default=["notification"], description="channel to listen system msg"
        ),
    ):

        _svc = _require_redis(serice)
        _channels = _parse_channels(channels)

        if _svc.active_streams >= _svc._config.MAX_CONCURRENT_STREAMS:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Trop de connexions simultanées",
            )

        pending = await _svc.flush_inbox(user["sub"])  # type:ignore

        async def stream_with_inbox():
            import json as _json

            # 1. Livraison de l'inbox (messages envoyés hors-ligne)
            for msg in pending:
                ch = msg.get("channel", "notification")
                yield (f"event: {ch}\ndata: {_json.dumps(msg, ensure_ascii=False)}\n\n")
            # 2. Stream live continu
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
        """
        Retourne et vide les messages en attente (envoyés quand l'user était hors-ligne).
        Utile pour les clients qui ne gardent pas le SSE ouvert en permanence.
        """
        redis = _require_redis(serice)
        user_id: str = current_user.get("sub", "")
        messages = await redis.flush_inbox(user_id)
        return {"messages": messages, "count": len(messages)}

    @router.get("/inbox/count", tags=["xpulse"])
    async def inbox_count(current_user: AuthPayload = Depends(get_current_user)):
        """Nombre de messages en attente sans les consommer."""
        redis = _require_redis(serice)
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
        """Publie un message ciblé sur un ou plusieurs channels."""
        redis = _require_redis(serice)
        parsed_channels = _parse_channels(channels)

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

    @router.post("/tennant/publish", tags=["xpulse"])
    async def publish_tennant(
        request: Request,
        user_id: str = Query(..., description="ID de l'utilisateur cible"),
        text: str = Query(..., description="Message à envoyer"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:tennant:publish")),
    ):
        """Publie un message ciblé sur un ou plusieurs channels."""
        redis = _require_redis(serice)
        parsed_channels = _parse_channels(channels)

        if caller is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="IPC caller non disponible.",
            )
        tennant = _extract_tenant_id(request)
        users = (
            await caller("auth", "xauth.users.list.tennant", {"tennant_id": tennant})
        ).get("users", [])
        if user_id not in users:
            logger.warning(f"User {user_id} {users} {tennant}")
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
        """Envoie un message à tous les abonnés sur un ou plusieurs channels.
        Publie sans user_id — le filtre SSE le délivre à tous les abonnés."""
        redis = _require_redis(serice)
        parsed_channels = _parse_channels(channels)

        results = await redis.publish_many(parsed_channels, {"text": text})
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {failed}",
            )
        logger.info("Broadcast : channels=%s", parsed_channels)
        return ok(channels=parsed_channels)

    @router.post("/tennant/broadcast", tags=["xpulse"])
    async def tennant_broadcast(
        request: Request,
        text: str = Query(..., description="Message à broadcaster"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:tennant:broadcast")),
    ):
        """Envoie un message à tous les abonnés sur un ou plusieurs channels.
        Publie sans user_id — le filtre SSE le délivre à tous les abonnés."""
        redis = _require_redis(serice)
        tennant_id = _extract_tenant_id(request)
        channel = f"tenant-{tennant_id}"

        response = await redis.publish(channel, {"text": text})
        if not response:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {channel}",
            )

        return ok(channels=[channel])

    return router
