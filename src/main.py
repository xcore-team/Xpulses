"""xpulse/src/main.py — Plugin SSE/Pub-Sub Redis pour xcore."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from xcore.kernel.api.rbac import AuthPayload, get_current_user, require_permission
from xcore.kernel.events import Event
from xcore.sdk import (
    AutoDispatchMixin,
    RoutedPlugin,
    RouterRegistry,
    TrustedBase,
    action,
    error,
    ok,
    validate_payload,
)

from .client import (
    InvalidChannel,
    RedisConfiguration,
    RedisPubSubManager,
    StreamLimitExceeded,
    validate_channels,
)

logger = logging.getLogger("xpulse.plugin")

router = RouterRegistry()

# ── Schémas IPC ───────────────────────────────────────────────────────────────

_PUBLISH_SCHEMA = {
    "channels": (list, ["notification"]),
    "user_id": (str, ...),
    "text": (str, ...),
}

_BROADCAST_SCHEMA = {
    "channels": (list, ["notification"]),
    "text": (str, ...),
}

_STREAM_SCHEMA = {
    "channels": (list, ["notification"]),
    "user_id": (str, ...),
}

_SUBSCRIBERS_SCHEMA = {
    "channel": (str, ...),
}

_EMAIL_SCHEMAS = {
    "to": (list, []),
    "subject": (str, ...),
    "template": (str, ...),
    "html_parser": (bool, True),
}


# ─────────────────────────────────────────────────────────────────────────────
# PLUGIN PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────


class Plugin(AutoDispatchMixin, RoutedPlugin, TrustedBase):
    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def on_load(self) -> None:
        self.event = self.ctx.events
        self.redis_server: RedisPubSubManager | None = None

        @self.ctx.health.register("xpulse.redis")
        async def redis_health_check():
            if not self.redis_server:
                return False, "Redis non configuré."
            alive = await self.redis_server.health_check()
            return alive, "Redis répond." if alive else "Redis ne répond pas."

        try:
            # self.ctx.env["channel"] = self.ctx.env["channel"].split(",")
            self.redis_server = RedisPubSubManager(
                RedisConfiguration.from_dict(self.ctx.env)
            )
            await self.redis_server.connect()
            logger.info("xpulse démarré — Redis prêt.")
        except Exception as exc:
            logger.error("xpulse : impossible d'initialiser Redis : %s", exc)
            logger.warning("xpulse démarré en mode dégradé (pas de Redis).")

        await self._register_event_handlers()

    async def on_unload(self) -> None:
        if self.redis_server:
            logger.info("xpulse : fermeture du pool Redis…")
            await self.redis_server.close()

    # ── Helpers ───────────────────────────────────────────────────────────

    def _require_redis(self) -> RedisPubSubManager:
        if not self.redis_server:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Service Redis indisponible.",
            )
        return self.redis_server

    def _parse_channels(self, raw: list[str]) -> list[str]:
        flat = []
        for c in raw:
            flat.extend(c.split(","))
        try:
            return validate_channels(flat)
        except InvalidChannel as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
            )

    def _normalize_channels(self, raw: Any) -> list[str]:
        if isinstance(raw, str):
            return raw.split(",")
        return list(raw) if raw else ["notification"]

    # ── Event handlers ────────────────────────────────────────────────────

    async def _register_event_handlers(self) -> None:

        @self.event.on("ext.notification.publish")
        async def handle_publish(event: Event):
            """
            Publie sur un ou plusieurs channels pour un user précis.
            Payload : { "channels": [...], "user_id": "...", ...données }
                  ou : { "channel": "...", "user_id": "...", ...données }
            Le payload complet est transmis au client SSE (user_id, event, submission_id, etc.)
            """
            if not self.redis_server:
                logger.warning("ext.notification.publish ignoré : Redis non disponible.")
                return [error("redis_unavailable")]

            data: dict = dict(event.data)
            raw_channels = data.pop("channels", None) or [data.pop("channel", "notification")]
            user_id = data.get("user_id")

            if not user_id:
                logger.warning("ext.notification.publish : user_id requis.")
                return [error("missing_fields")]

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning("ext.notification.publish : channels invalides : %s", exc)
                return [error(str(exc))]

            # Publie le payload complet (user_id + toutes les données événementielles)
            results = await self.redis_server.publish_many(channels, data)
            ok_channels = [ch for ch, s in results.items() if s]
            fail_channels = [ch for ch, s in results.items() if not s]
            if fail_channels:
                logger.warning("ext.notification.publish : channels en échec : %s", fail_channels)
            return [ok(channels=ok_channels, failed=fail_channels)]

        @self.event.on("ext.notification.broadcast")
        async def handle_broadcast(event: Event):
            """
            Broadcast vers tous les subscribers actifs.
            Payload : { "channels": [...], "text": "...", ...données }
            Le payload complet (sans user_id) est publié — le stream SSE le délivre
            à tous les abonnés du channel (messages sans user_id = broadcast).
            """
            if not self.redis_server:
                return [error("redis_unavailable")]

            data: dict = dict(event.data)
            raw_channels = data.pop("channels", ["notification"])

            if not data:
                logger.warning("ext.notification.broadcast : payload vide.")
                return [error("missing_payload")]

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning("broadcast : channels invalides : %s", exc)
                return [error(str(exc))]

            # Publie sans user_id → le stream SSE délivre à tous les abonnés
            results = await self.redis_server.publish_many(channels, data)
            ok_channels = [ch for ch, s in results.items() if s]
            fail_channels = [ch for ch, s in results.items() if not s]
            if fail_channels:
                logger.warning("ext.notification.broadcast : channels en échec : %s", fail_channels)
            return [ok(channels=ok_channels, failed=fail_channels)]

    # ── Routes HTTP ───────────────────────────────────────────────────────

    @router.get("/stream", tags=["xpulse"])
    async def get_stream(
        self,
        current_user: AuthPayload = Depends(get_current_user),
        channels: list[str] = Query(
            default=["notification"],
            description="Channel(s) à écouter. Ex: ?channels=notification&channels=alerts",
        ),
    ):
        """
        SSE multi-channel — ouvre un stream pour l'utilisateur authentifié.

        Chaque event SSE est typé par le nom du channel :
            event: notification
            data: {"channel": "notification", "user_id": "...", "text": "..."}

        Usage JS :
            const src = new EventSource('/stream?channels=notification,alerts', { withCredentials: true });
            src.addEventListener('notification', e => console.log(JSON.parse(e.data)));
        """
        redis = self._require_redis()
        user_id: str = current_user.get("sub", None)
        
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Token invalide.{current_user}"
            )
            

        parsed_channels = self._parse_channels(channels)

        try:
            generator = redis.stream(channels=parsed_channels, user_id=user_id)
        except StreamLimitExceeded as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)
            )

        return StreamingResponse(
            generator,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @router.post("/publish", tags=["xpulse"])
    async def publish(
        self,
        user_id: str = Query(..., description="ID de l'utilisateur cible"),
        text: str = Query(..., description="Message à envoyer"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:publish")),
    ):
        """Publie un message ciblé sur un ou plusieurs channels."""
        redis = self._require_redis()
        parsed_channels = self._parse_channels(channels)

        results = await redis.publish_many(
            parsed_channels, {"user_id": user_id, "text": text}
        )
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Publish échoué sur : {failed}",
            )
        return {"status": "ok", "channels": parsed_channels}

    @router.post("/broadcast", tags=["xpulse"])
    async def broadcast(
        self,
        text: str = Query(..., description="Message à broadcaster"),
        channels: list[str] = Query(default=["notification"]),
        _: AuthPayload = Depends(require_permission("xpulse:broadcast")),
    ):
        """Envoie un message à tous les abonnés sur un ou plusieurs channels.
        Publie sans user_id — le filtre SSE le délivre à tous les abonnés."""
        redis = self._require_redis()
        parsed_channels = self._parse_channels(channels)

        results = await redis.publish_many(parsed_channels, {"text": text})
        failed = [ch for ch, s in results.items() if not s]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Broadcast échoué sur : {failed}",
            )
        logger.info("Broadcast : channels=%s", parsed_channels)
        return {"status": "ok", "channels": parsed_channels}

    @router.post("/test-emit", tags=["xpulse"])
    async def test_emit(self):
        await self.ctx.events.emit(
            "stock.updated",
            {
                "product_id": 888,
                "name": "TEST XPULSE EMIT",
                "quantity_before": 20,
                "quantity_after": 3,
                "safety_stock": 10,
            },
        )
        return {"status": "event_emitted_from_xpulse"}

    # ── Actions IPC ───────────────────────────────────────────────────────

    @action("xpulse.publish")
    @validate_payload(_PUBLISH_SCHEMA, type_response="model", unset=False)
    async def ipc_publish(self, payload) -> dict:
        """
        Publie un message ciblé sur un ou plusieurs channels.
        Payload : { "user_id": "...", "text": "...", "channels": [...] }
        """
        if not self.redis_server:
            return error("redis_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await self.redis_server.publish_many(
            channels, {"user_id": payload.user_id, "text": payload.text}
        )
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.broadcast")
    @validate_payload(_BROADCAST_SCHEMA, type_response="model", unset=False)
    async def ipc_broadcast(self, payload) -> dict:
        """
        Broadcast un message à tous les abonnés des channels.
        Payload : { "text": "...", "channels": [...] }
        Publie sans user_id → délivré à tous les abonnés (filtre SSE ignoré).
        """
        if not self.redis_server:
            return error("redis_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await self.redis_server.publish_many(channels, {"text": payload.text})
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.stream")
    @validate_payload(_STREAM_SCHEMA, type_response="model", unset=False)
    async def ipc_stream(self, payload) -> dict:
        """
        Publie un event de notification sur des channels pour un user donné.
        Payload : { "user_id": "...", "channels": [...] }
        """
        if not self.redis_server:
            return error("redis_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await self.redis_server.publish_many(
            channels, {"user_id": payload.user_id}
        )
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.subscribers")
    @validate_payload(_SUBSCRIBERS_SCHEMA, type_response="model", unset=False)
    async def ipc_subscribers(self, payload) -> dict:
        """
        Retourne le nombre de streams actifs.
        Payload : { "channel": "..." }
        """
        if not self.redis_server:
            return error("redis_unavailable")
        return ok(
            channel=payload.channel, active_streams=self.redis_server.active_streams
        )

    @action("xpulse.email")
    @validate_payload(_EMAIL_SCHEMAS, type_response="model", unset=False)
    async def send_and_forget_mail(self, payload):
        try:
            email = self.get_service("ext.email")
            response = email.queue(
                to=payload.to,
                subject=payload.subject,
                is_html=payload.html_parser,
                body=payload.template,
            )

            return (
                ok(message="email as been send", response=response)
                if response
                else error(
                    "email as not send",
                    error="systeme as not deternine why",
                    code="Unknow error",
                )
            )

        except Exception:
            return error("service mail as not found", "NoT Found")

    # ── Router ────────────────────────────────────────────────────────────

    def get_router(self) -> Any | None:
        return self.RouterIn()
