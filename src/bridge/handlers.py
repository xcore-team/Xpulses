"""
xpulse/src/bridge/handlers.py — Pont de notifications inter-plugins (xauth).
"""

from __future__ import annotations

import html
from functools import partial
from typing import TYPE_CHECKING

from xcore.sdk import get_logger

if TYPE_CHECKING:
    from xcore.kernel.events import Event, EventBus

    from extensions.pubsub.service import PubSubClient

logger = get_logger("xpulse.bridge")


# ── Helpers ───────────────────────────────────────────────────────────────

# `text` est html.escape()-é avant publication : défense en profondeur, pas
# une réponse à un XSS confirmé (aucun sink dangerouslySetInnerHTML/innerHTML
# côté frontend actuel) — mais `text` interpole des champs semi-contrôlés
# (email, slug, plan_id) et XPulse ne devrait pas faire reposer son intégrité
# sur le fait qu'aucun consommateur, présent ou futur, ne rend jamais ce
# contenu en HTML brut (audit XPulse Constat 5).


def _tenant_channel(tenant_id: str) -> str:
    return f"tenant-{tenant_id}"


async def _notify_user(
    svc: "PubSubClient",
    user_id: str,
    event_type: str,
    text: str,
    **extra,
) -> None:
    if not user_id:
        return
    try:
        await svc.publish(
            "notification",
            {"user_id": user_id, "event_type": event_type, "text": html.escape(text), **extra},
        )
    except Exception as exc:
        logger.warning("_notify_user(%s) échoué : %s", event_type, exc)


async def _notify_tenant(
    svc: "PubSubClient",
    tenant_id: str | None,
    event_type: str,
    text: str,
    **extra,
) -> None:
    if not tenant_id:
        return
    try:
        await svc.publish(
            _tenant_channel(tenant_id),
            {"event_type": event_type, "text": html.escape(text), "tenant_id": tenant_id, **extra},
        )
    except Exception as exc:
        logger.warning("_notify_tenant(%s) échoué : %s", event_type, exc)


# ── xauth : alertes sécurité utilisateur ─────────────────────────────────


async def _on_login(event: "Event", svc: "PubSubClient") -> None:
    user_id = event.data.get("user_id", "")
    ip = event.data.get("ip") or "inconnue"
    if user_id:
        await _notify_user(
            svc, user_id, "security.new_login",
            f"Nouvelle connexion depuis {ip}.",
            ip_address=ip, tenant_id=event.data.get("tenant_id"),
        )


async def _on_mfa_enabled(event: "Event", svc: "PubSubClient") -> None:
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.mfa_enabled",
        "Authentification à deux facteurs activée sur votre compte.",
    )


async def _on_mfa_disabled(event: "Event", svc: "PubSubClient") -> None:
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.mfa_disabled",
        "⚠️ Authentification à deux facteurs désactivée.",
    )


async def _on_password_changed(event: "Event", svc: "PubSubClient") -> None:
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.password_changed",
        "⚠️ Votre mot de passe a été modifié.",
    )


async def _on_password_reset(event: "Event", svc: "PubSubClient") -> None:
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.password_reset",
        "⚠️ Réinitialisation de mot de passe effectuée.",
    )


async def _on_oauth_linked(event: "Event", svc: "PubSubClient") -> None:
    provider = event.data.get("provider", "externe")
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.oauth_linked",
        f"Compte {provider} lié à votre profil.",
        provider=provider,
    )


async def _on_oauth_unlinked(event: "Event", svc: "PubSubClient") -> None:
    provider = event.data.get("provider", "externe")
    await _notify_user(
        svc, event.data.get("user_id", ""),
        "security.oauth_unlinked",
        f"Compte {provider} délié de votre profil.",
        provider=provider,
    )


# ── xauth : activité tenant ──────────────────────────────────────────────


async def _on_invite_accepted(event: "Event", svc: "PubSubClient") -> None:
    tenant_id = event.data.get("tenant_id")
    user_id = event.data.get("user_id")
    if user_id:
        await _notify_user(
            svc, user_id, "tenant.joined",
            "Vous avez rejoint un nouveau tenant.",
            tenant_id=tenant_id,
        )
    if tenant_id:
        await _notify_tenant(
            svc, tenant_id, "tenant.member_joined",
            "Un nouveau membre a rejoint le tenant.",
            user_id=user_id,
        )


async def _on_invite_created(event: "Event", svc: "PubSubClient") -> None:
    tenant_id = event.data.get("tenant_id")
    email = event.data.get("email", "")
    if tenant_id:
        await _notify_tenant(
            svc, tenant_id, "tenant.invite_sent",
            f"Invitation envoyée à {email}.",
            email=email,
        )


async def _on_tenant_created(event: "Event", svc: "PubSubClient") -> None:
    owner_id = event.data.get("owner_id")
    slug = event.data.get("slug", "")
    if owner_id:
        await _notify_user(
            svc, owner_id, "tenant.created",
            f"Tenant « {slug} » créé avec succès.",
            slug=slug, tenant_id=event.data.get("tenant_id"),
        )


# ── Registreur principal ─────────────────────────────────────────────────


def register_bridge(event_bus: "EventBus", svc: "PubSubClient") -> None:
    from .billing import HANDLERS as BILLING_HANDLERS

    auth_handlers: list[tuple[str, ...]] = [
        ("xauth.auth.login", _on_login, "xauth"),
        ("xauth.mfa.enabled", _on_mfa_enabled, "xauth"),
        ("xauth.mfa.disabled", _on_mfa_disabled, "xauth"),
        ("xauth.password.changed", _on_password_changed, "xauth"),
        ("xauth.password.reset_completed", _on_password_reset, "xauth"),
        ("xauth.oauth.linked", _on_oauth_linked, "xauth"),
        ("xauth.oauth.unlinked", _on_oauth_unlinked, "xauth"),
        ("xauth.invite.accepted", _on_invite_accepted, "xauth"),
        ("xauth.invite.created", _on_invite_created, "xauth"),
        ("xauth.tenant.created", _on_tenant_created, "xauth"),
    ]

    counts: dict[str, int] = {}
    for event_name, handler, domain in auth_handlers + BILLING_HANDLERS:
        event_bus.on(event_name)(partial(handler, svc=svc))
        counts[domain] = counts.get(domain, 0) + 1

    parts = [f"{d} ({c})" for d, c in sorted(counts.items())]
    logger.info("xpulse bridge enregistré — %s events bridgés", " + ".join(parts))
