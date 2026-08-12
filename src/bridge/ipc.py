"""xpulse/src/bridge/ipc.py — Actions IPC du plugin xpulse."""

from __future__ import annotations

import html
from typing import Any

from xcore.sdk import action, error, get_logger, ok, schema

from ..utils import InvalidChannel, validate_channels

logger = get_logger("xpulse.ipc")


class IPCActionsMixin:
    """Mixin qui expose les actions IPC xpulse."""

    # ── Helpers ───────────────────────────────────────────────────────────

    def _normalize_channels(self, raw: Any) -> list[str]:
        if isinstance(raw, str):
            return raw.split(",")
        return list(raw) if raw else ["notification"]

    # ── Actions IPC ───────────────────────────────────────────────────────

    @action("xpulse.publish")
    @schema(
        version="1.0",
        input={"channels": (list, ["notification"]), "user_id": (str, ...), "text": (str, ...)},
        output={"channels": list, "failed": list},
        type_response="model",
        unset=False,
    )
    async def ipc_publish(self, payload) -> dict:
        svc = getattr(self, "_pubsub", None)
        if not svc:
            return error("pubsub_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await svc.publish_many(
            channels, {"user_id": payload.user_id, "text": html.escape(payload.text)}
        )
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.broadcast")
    @schema(
        version="1.0",
        input={"channels": (list, ["notification"]), "text": (str, ...)},
        output={"channels": list, "failed": list},
        type_response="model",
        unset=False,
    )
    async def ipc_broadcast(self, payload) -> dict:
        svc = getattr(self, "_pubsub", None)
        if not svc:
            return error("pubsub_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await svc.publish_many(channels, {"text": html.escape(payload.text)})
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.stream")
    @schema(
        version="1.0",
        input={"channels": (list, ["notification"]), "user_id": (str, ...)},
        output={"channels": list, "failed": list},
        type_response="model",
        unset=False,
    )
    async def ipc_stream(self, payload) -> dict:
        svc = getattr(self, "_pubsub", None)
        if not svc:
            return error("pubsub_unavailable")

        try:
            channels = validate_channels(self._normalize_channels(payload.channels))
        except InvalidChannel as exc:
            return error(str(exc), code="invalid_channel")

        results = await svc.publish_many(
            channels, {"user_id": payload.user_id}
        )
        failed = [ch for ch, s in results.items() if not s]
        return ok(channels=[ch for ch in channels if ch not in failed], failed=failed)

    @action("xpulse.subscribers")
    @schema(
        version="1.0",
        input={"channel": (str, ...)},
        output={"channel": str, "active_streams": int},
        type_response="model",
        unset=False,
    )
    async def ipc_subscribers(self, payload) -> dict:
        svc = getattr(self, "_pubsub", None)
        if not svc:
            return error("pubsub_unavailable")
        return ok(
            channel=payload.channel, active_streams=svc.active_streams
        )

    @action("xpulse.email")
    @schema(
        version="1.0",
        input={"to": (list, []), "subject": (str, ...), "template": (str, ...), "html_parser": (bool, True)},
        output={"message": str},
        type_response="model",
        unset=False,
    )
    async def send_and_forget_mail(self, payload):
        try:
            email = self.get_service("ext.email")
        except Exception as exc:
            logger.warning("xpulse.email : service ext.email indisponible : %s", exc)
            return error("Service email indisponible.", code="service_unavailable")

        try:
            response = email.queue(
                to=payload.to,
                subject=payload.subject,
                is_html=payload.html_parser,
                body=payload.template,
            )
        except Exception as exc:
            logger.warning("xpulse.email : envoi échoué : %s", exc)
            return error("Échec de l'envoi de l'email.", code="send_failed")

        if not response:
            return error("Échec de l'envoi de l'email.", code="send_failed")
        return ok(message="Email envoyé.", response=response)
