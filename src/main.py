"""xpulse/src/main.py — Plugin SSE/Pub-Sub Redis pour xcore."""

from __future__ import annotations

import logging
from typing import Any

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
    schema,
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

    # ── License email helpers ─────────────────────────────────────────────

    async def _get_tenant_emails(self, tenant_id: str) -> list[str]:
        """Resolve active member emails for a tenant via xauth IPC."""
        try:
            result = await self.event.emit(
                "xauth.get_tenant_members", {"tenant_id": tenant_id}
            )
            if not result or not result[0]:
                return []
            members = result[0].get("members", [])
            return [m["email"] for m in members if m.get("email")]
        except Exception as exc:
            logger.warning(
                "xpulse: résolution emails tenant %s échouée: %s",
                tenant_id,
                exc,
            )
            return []

    async def _send_license_email(self, to: list[str], subject: str, body: str) -> None:
        if not to:
            return
        try:
            email = self.get_service("ext.email")
            email.queue(to=to, subject=subject, is_html=False, body=body)
        except Exception as exc:
            logger.warning("xpulse: envoi email licence échoué: %s", exc)

    # ── Event handlers ────────────────────────────────────────────────────

    async def _register_event_handlers(self) -> None:

        @self.event.on("xlicense.license.created")
        async def handle_license_created(event: Event):
            tenant_id = event.data.get("tenant_id", "")
            state = event.data.get("state", "")
            expires_at = event.data.get("expires_at", "")
            to = await self._get_tenant_emails(tenant_id)
            await self._send_license_email(
                to=to,
                subject="Votre licence a été créée",
                body=(
                    f"Bonjour,\n\n"
                    f"Votre licence ({state}) a été créée avec succès.\n"
                    f"Expiration : {expires_at or 'illimitée'}\n\n"
                    f"L'équipe"
                ),
            )

        @self.event.on("xlicense.license.transitioned")
        async def handle_license_transitioned(event: Event):
            tenant_id = event.data.get("tenant_id", "")
            from_state = event.data.get("from_state", "")
            to_state = event.data.get("to_state", "")
            reason = event.data.get("reason", "")
            to = await self._get_tenant_emails(tenant_id)
            await self._send_license_email(
                to=to,
                subject=f"Votre licence est maintenant : {to_state}",
                body=(
                    f"Bonjour,\n\n"
                    f"L'état de votre licence a changé : "
                    f"{from_state} → {to_state}.\n"
                    f"Raison : {reason or 'non précisée'}\n\n"
                    f"L'équipe"
                ),
            )

        @self.event.on("xlicense.license.renewed")
        async def handle_license_renewed(event: Event):
            tenant_id = event.data.get("tenant_id", "")
            expires_at = event.data.get("expires_at", "")
            to = await self._get_tenant_emails(tenant_id)
            await self._send_license_email(
                to=to,
                subject="Votre licence a été renouvelée",
                body=(
                    f"Bonjour,\n\n"
                    f"Votre licence a été renouvelée avec succès.\n"
                    f"Nouvelle expiration : {expires_at or 'illimitée'}\n\n"
                    f"L'équipe"
                ),
            )

        @self.event.on("xlicense.license.expired")
        async def handle_license_expired(event: Event):
            tenant_id = event.data.get("tenant_id", "")
            to = await self._get_tenant_emails(tenant_id)
            await self._send_license_email(
                to=to,
                subject="Votre licence a expiré",
                body=(
                    "Bonjour,\n\n"
                    "Votre licence a expiré. "
                    "Veuillez la renouveler pour continuer "
                    "à accéder au service.\n\n"
                    "L'équipe"
                ),
            )

        @self.event.on("ext.notification.publish")
        async def handle_publish(event: Event):
            """
            Publie sur un ou plusieurs channels pour un user précis.
            Payload : { "channels": [...], "user_id": "...", "text": "..." }
                  ou : { "channel": "...", "user_id": "...", "text": "..." }
            """
            if not self.redis_server:
                logger.warning(
                    "ext.notification.publish ignoré : Redis non disponible."
                )
                return [error("redis_unavailable")]

            data: dict = dict(event.data)
            raw_channels = data.pop("channels", None) or [
                data.pop("channel", "notification")
            ]
            user_id = data.get("user_id")
            text = data.get("text", "")

            if not user_id or not text:
                logger.warning(
                    "ext.notification.publish : user_id et text sont requis."
                )
                return [error("missing_fields")]

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning(
                    "ext.notification.publish : channels invalides : %s", exc
                )
                return [error(str(exc))]

            results = await self.redis_server.publish_many(
                channels, {"user_id": user_id, "text": text}
            )
            ok_channels = [ch for ch, s in results.items() if s]
            fail_channels = [ch for ch, s in results.items() if not s]
            if fail_channels:
                logger.warning(
                    "ext.notification.publish : channels en échec : %s", fail_channels
                )
            return [ok(channels=ok_channels, failed=fail_channels)]

        @self.event.on("ext.notification.broadcast")
        async def handle_broadcast(event: Event):
            """
            Broadcast vers tous les users actifs.
            Payload : { "channels": [...], "text": "..." }
            """
            if not self.redis_server:
                return [error("redis_unavailable")]

            data: dict = event.data
            raw_channels = data.get("channels", ["notification"])
            text = data.get("text", "")

            if not text:
                logger.warning("ext.notification.broadcast : text requis.")
                return [error("missing_text")]

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning("broadcast : channels invalides : %s", exc)
                return [error(str(exc))]

            try:
                response = await self.event.emit("auth.get.user.ids", {})
                user_ids = list(response[0]) if response and response[0] else []
            except Exception as exc:
                logger.error(
                    "broadcast : impossible de récupérer les user IDs : %s", exc
                )
                return [error("cannot_fetch_users")]

            for uid in user_ids:
                await self.redis_server.publish_many(
                    channels, {"user_id": uid, "text": text}
                )

            return [ok(sent=len(user_ids), channels=channels)]

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
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide."
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
        """Envoie un message à tous les utilisateurs sur un ou plusieurs channels."""
        redis = self._require_redis()
        parsed_channels = self._parse_channels(channels)

        try:
            response = await self.event.emit("auth.get.user.ids", {})
            user_ids = list(response[0]) if response and response[0] else []
        except Exception as exc:
            logger.error(
                "broadcast HTTP : impossible de récupérer les user IDs : %s", exc
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erreur lors de la récupération des utilisateurs.",
            )

        if not user_ids:
            return {"status": "ok", "sent": 0, "channels": parsed_channels}

        total_errors = 0
        for uid in user_ids:
            results = await redis.publish_many(
                parsed_channels, {"user_id": uid, "text": text}
            )
            total_errors += sum(1 for s in results.values() if not s)

        logger.info(
            "Broadcast : %d users × %d channels, %d erreurs.",
            len(user_ids),
            len(parsed_channels),
            total_errors,
        )
        return {
            "status": "ok",
            "sent": len(user_ids),
            "channels": parsed_channels,
            "errors": total_errors,
        }

    # ── Actions IPC ───────────────────────────────────────────────────────

    @action("xpulse.publish")
    @schema(
        version="0.1.0",
        input=_PUBLISH_SCHEMA,
        type_response="model",
        unset=False,
        description="Publie un message ciblé sur un ou plusieurs channels.",
    )
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
    @schema(
        version="0.1.0",
        input=_BROADCAST_SCHEMA,
        type_response="model",
        unset=False,
        description="Broadcast un message à tous les users actifs.",
    )
    async def ipc_broadcast(self, payload) -> dict:
        """
        Broadcast un message à tous les users actifs.
        Payload : { "text": "...", "channels": [...] }
        """
        if not self.redis_server:
            return error("redis_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        try:
            response = await self.event.emit("auth.get.user.ids", {})
            user_ids = list(response[0]) if response and response[0] else []
        except Exception as exc:
            logger.error("xpulse.broadcast : user IDs introuvables : %s", exc)
            return error("cannot_fetch_users")

        for uid in user_ids:
            await self.redis_server.publish_many(
                channels, {"user_id": uid, "text": payload.text}
            )

        return ok(sent=len(user_ids), channels=channels)

    @action("xpulse.subscribers")
    @schema(
        version="0.1.0",
        input=_SUBSCRIBERS_SCHEMA,
        type_response="model",
        unset=False,
        description="Retourne le nombre de streams actifs.",
    )
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
    @schema(
        version="0.1.0",
        input=_EMAIL_SCHEMAS,
        type_response="model",
        unset=False,
        description="Envoie d'email via xpulse",
    )
    async def send_and_forget_mail(self, payload):
        try:
            email: _EmailService = self.get_service("ext.email")
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
