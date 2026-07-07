"""xpulse/src/main.py — Plugin SSE/Pub-Sub Redis pour xcore."""

from __future__ import annotations

import logging
from typing import Any

from xcore.kernel.events import Event
from xcore.sdk import (
    AutoDispatchMixin,
    EventMixin,
    ObservabilityMixin,
    RouterRegistry,
    TrustedBase,
    error,
    get_logger,
    health_check,
    ok,
    on_event,
)

from .bridge import register_bridge
from .bridge.ipc import IPCActionsMixin
from .routes import builder_router

logger = get_logger("xpulse.plugin")

router = RouterRegistry()


class Plugin(
    IPCActionsMixin, AutoDispatchMixin, EventMixin, ObservabilityMixin, TrustedBase
):
    async def on_load(self) -> None:
        self.event = self.ctx.events
        self._pubsub: Any = None
        try:
            self._pubsub = self.get_service("ext.pubsub")
            logger.info("xpulse démarré — ext.pubsub connecté.")
        except Exception as exc:
            logger.error("xpulse : ext.pubsub indisponible : %s", exc)
            logger.warning("xpulse démarré en mode dégradé (pas de pubsub).")

        if self._pubsub:
            register_bridge(self.ctx.events, self._pubsub)

        self.app = builder_router(self._pubsub, self.call_plugin)
        await self._declare_rbac()

    async def on_unload(self) -> None:
        if self._pubsub:
            logger.info("xpulse : fermeture du pubsub…")
            await self._pubsub.shutdown()

    # ── Event handlers ────────────────────────────────────────────────────
    @on_event("ext.notification.publish")
    async def handle_publish(self, event: Event):
        if not self._pubsub:
            logger.warning("ext.notification.publish ignoré : pubsub non disponible.")
            return [error("pubsub_unavailable")]
        data: dict = dict(event.data)
        raw_channels = data.pop("channels", None) or [
            data.pop("channel", "notification")
        ]
        user_id = data.get("user_id")
        if not user_id:
            logger.warning("ext.notification.publish : user_id requis.")
            return [error("missing_fields")]

        channels = raw_channels if isinstance(raw_channels, list) else [raw_channels]

        results = await self._pubsub.publish_many(channels, data)
        ok_channels = [ch for ch, s in results.items() if s]
        fail_channels = [ch for ch, s in results.items() if not s]
        if fail_channels:
            logger.warning(
                "ext.notification.publish : channels en échec : %s", fail_channels
            )
        return [ok(channels=ok_channels, failed=fail_channels)]

    @on_event("ext.notification.broadcast")
    async def handle_broadcast(self, event: Event):
        if not self._pubsub:
            return [error("pubsub_unavailable")]

        data: dict = dict(event.data)
        raw_channels = data.pop("channels", ["notification"])

        if not data:
            logger.warning("ext.notification.broadcast : payload vide.")
            return [error("missing_payload")]

        channels = raw_channels if isinstance(raw_channels, list) else [raw_channels]

        results = await self._pubsub.publish_many(channels, data)
        ok_channels = [ch for ch, s in results.items() if s]
        fail_channels = [ch for ch, s in results.items() if not s]
        if fail_channels:
            logger.warning(
                "ext.notification.broadcast : channels en échec : %s", fail_channels
            )
        return [ok(channels=ok_channels, failed=fail_channels)]

    # ── Router ────────────────────────────────────────────────────────────

    def get_router(self) -> Any | None:
        return self.app

    @health_check("xpulse.checker")
    async def _redis_health_check(self):
        if not self._pubsub:
            return False, "Pubsub non configuré."
        ok, msg = await self._pubsub.health_check()
        return ok, msg

    async def _declare_rbac(self) -> None:
        rbac = (self.ctx.config or {}).get("rbac") or {}
        grants = rbac.get("grants") or []
        if not grants:
            return
        try:
            await self.ctx.events.emit(
                "rbac.declare",
                {"plugin": "xpulse", "grants": grants},
                source="xpulse",
            )
            logger.info("[xpulse] rbac.declare émis (%d grant(s))", len(grants))
        except Exception as exc:
            logger.warning("[xpulse] rbac.declare ignoré : %s", exc)
