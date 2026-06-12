"""
xpulse/src/bridge.py — Pont de notifications inter-plugins.

Écoute les événements domaine de xauth / xlicense / xpayproxy et les traduit
en notifications SSE via le RedisPubSubManager.

Deux types de canaux :
  • "notification"         — personnel, filtré par user_id (alertes sécurité user)
  • "tenant-<tenant_id>"   — broadcast tenant, tous les membres abonnés reçoivent
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from xcore.sdk import get_logger

if TYPE_CHECKING:
    from xcore.kernel.events import Event, EventBus

    from .client import RedisPubSubManager

logger = get_logger("xpulse.bridge")


def _tenant_channel(tenant_id: str) -> str:
    """Canal Redis scopé au tenant. Format valide pour validate_channels."""
    return f"tenant-{tenant_id}"


async def _notify_user(
    redis: "RedisPubSubManager",
    user_id: str,
    event_type: str,
    text: str,
    **extra: Any,
) -> None:
    """Publie une notification personnelle sur le canal 'notification'."""
    if not user_id:
        return
    try:
        await redis.publish(
            "notification",
            {
                "user_id": user_id,
                "event_type": event_type,
                "text": text,
                **extra,
            },
        )
    except Exception as exc:
        logger.warning("_notify_user(%s) échoué : %s", event_type, exc)


async def _notify_tenant(
    redis: "RedisPubSubManager",
    tenant_id: str | None,
    event_type: str,
    text: str,
    **extra: Any,
) -> None:
    """
    Publie une notification sur le canal tenant-<id>.
    Sans user_id → le filtre SSE la délivre à tous les membres abonnés.
    """
    if not tenant_id:
        return
    try:
        await redis.publish(
            _tenant_channel(tenant_id),
            {
                "event_type": event_type,
                "text": text,
                "tenant_id": tenant_id,
                **extra,
            },
        )
    except Exception as exc:
        logger.warning("_notify_tenant(%s) échoué : %s", event_type, exc)


# ── Messages par type de transition de licence ────────────────────────────────

_LICENSE_TRANSITION_MESSAGES: dict[str, tuple[str, str]] = {
    "active": ("license.activated", "Votre licence est maintenant active."),
    "suspended": (
        "license.suspended",
        "⚠️ Licence suspendue — vérifiez votre abonnement.",
    ),
    "expired": ("license.expired", "⚠️ Votre licence a expiré."),
    "trial": ("license.trial", "Période d'essai démarrée."),
    "cancelled": ("license.cancelled", "Licence annulée."),
}


# ── Registreur principal ──────────────────────────────────────────────────────


def register_bridge(event_bus: "EventBus", redis: "RedisPubSubManager") -> None:
    """
    Enregistre tous les handlers de bridge sur l'event bus.
    Appelé une seule fois depuis Plugin.on_load, après la connexion Redis.
    Tous les handlers sont tolérants aux erreurs : un bug de bridge ne doit
    jamais faire crasher le plugin.
    """

    # ── xauth : alertes sécurité utilisateur ─────────────────────────────────

    @event_bus.on("xauth.auth.login")
    async def _on_login(event: "Event") -> None:
        r = redis
        if not r:
            return
        user_id = event.data.get("user_id", "")
        ip = event.data.get("ip") or "inconnue"
        if user_id:
            await _notify_user(
                r,
                user_id,
                "security.new_login",
                f"Nouvelle connexion depuis {ip}.",
                ip_address=ip,
                tenant_id=event.data.get("tenant_id"),
            )

    @event_bus.on("xauth.mfa.enabled")
    async def _on_mfa_enabled(event: "Event") -> None:
        r = redis
        if r:
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.mfa_enabled",
                "Authentification à deux facteurs activée sur votre compte.",
            )

    @event_bus.on("xauth.mfa.disabled")
    async def _on_mfa_disabled(event: "Event") -> None:
        r = redis
        if r:
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.mfa_disabled",
                "⚠️ Authentification à deux facteurs désactivée.",
            )

    @event_bus.on("xauth.password.changed")
    async def _on_password_changed(event: "Event") -> None:
        r = redis
        if r:
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.password_changed",
                "⚠️ Votre mot de passe a été modifié.",
            )

    @event_bus.on("xauth.password.reset_completed")
    async def _on_password_reset(event: "Event") -> None:
        r = redis
        if r:
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.password_reset",
                "⚠️ Réinitialisation de mot de passe effectuée.",
            )

    @event_bus.on("xauth.oauth.linked")
    async def _on_oauth_linked(event: "Event") -> None:
        r = redis
        if r:
            provider = event.data.get("provider", "externe")
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.oauth_linked",
                f"Compte {provider} lié à votre profil.",
                provider=provider,
            )

    @event_bus.on("xauth.oauth.unlinked")
    async def _on_oauth_unlinked(event: "Event") -> None:
        r = redis
        if r:
            provider = event.data.get("provider", "externe")
            await _notify_user(
                r,
                event.data.get("user_id", ""),
                "security.oauth_unlinked",
                f"Compte {provider} délié de votre profil.",
                provider=provider,
            )

    # ── xauth : activité tenant ───────────────────────────────────────────────

    @event_bus.on("xauth.invite.accepted")
    async def _on_invite_accepted(event: "Event") -> None:
        r = redis
        if not r:
            return
        tenant_id = event.data.get("tenant_id")
        user_id = event.data.get("user_id")
        # Notifie le nouveau membre
        if user_id:
            await _notify_user(
                r,
                user_id,
                "tenant.joined",
                "Vous avez rejoint un nouveau tenant.",
                tenant_id=tenant_id,
            )
        # Notifie le tenant (pour les admins qui regardent le dashboard)
        if tenant_id:
            await _notify_tenant(
                r,
                tenant_id,
                "tenant.member_joined",
                "Un nouveau membre a rejoint le tenant.",
                user_id=user_id,
            )

    @event_bus.on("xauth.invite.created")
    async def _on_invite_created(event: "Event") -> None:
        r = redis
        if not r:
            return
        tenant_id = event.data.get("tenant_id")
        email = event.data.get("email", "")
        if tenant_id:
            await _notify_tenant(
                r,
                tenant_id,
                "tenant.invite_sent",
                f"Invitation envoyée à {email}.",
                email=email,
            )

    @event_bus.on("xauth.tenant.created")
    async def _on_tenant_created(event: "Event") -> None:
        r = redis
        if not r:
            return
        owner_id = event.data.get("owner_id")
        slug = event.data.get("slug", "")
        if owner_id:
            await _notify_user(
                r,
                owner_id,
                "tenant.created",
                f"Tenant « {slug} » créé avec succès.",
                slug=slug,
                tenant_id=event.data.get("tenant_id"),
            )

    # ── xlicense : cycle de vie de la licence ─────────────────────────────────

    @event_bus.on("xlicense.license.created")
    async def _on_license_created(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "license.created",
                "Licence créée pour votre tenant.",
                license_id=event.data.get("license_id"),
                plan_id=event.data.get("plan_id"),
                state=event.data.get("state"),
            )

    @event_bus.on("xlicense.license.transitioned")
    async def _on_license_transitioned(event: "Event") -> None:
        r = redis
        if not r:
            return
        to_state = event.data.get("to_state", "")
        evt_type, text = _LICENSE_TRANSITION_MESSAGES.get(
            to_state, ("license.transitioned", f"Statut de licence : {to_state}.")
        )
        await _notify_tenant(
            r,
            event.data.get("tenant_id"),
            evt_type,
            text,
            license_id=event.data.get("license_id"),
            from_state=event.data.get("from_state"),
            to_state=to_state,
            reason=event.data.get("reason"),
        )

    @event_bus.on("xlicense.license.renewed")
    async def _on_license_renewed(event: "Event") -> None:
        r = redis
        if r:
            expires_at = event.data.get("expires_at") or "–"
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "license.renewed",
                f"Licence renouvelée jusqu'au {expires_at}.",
                license_id=event.data.get("license_id"),
                expires_at=expires_at,
            )

    @event_bus.on("xlicense.license.expired")
    async def _on_license_expired(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "license.expired",
                "⚠️ Votre licence a expiré. Renouvelez pour continuer à accéder au service.",
                license_id=event.data.get("license_id"),
            )

    @event_bus.on("xlicense.license.plan_changed")
    async def _on_license_plan_changed(event: "Event") -> None:
        r = redis
        if r:
            plan_id = event.data.get("plan_id", "")
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "license.plan_changed",
                f"Plan changé vers « {plan_id} ».",
                license_id=event.data.get("license_id"),
                plan_id=plan_id,
                reason=event.data.get("reason"),
            )

    @event_bus.on("xlicense.license.key_rotated")
    async def _on_license_key_rotated(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "license.key_rotated",
                "Clé de licence régénérée.",
                license_id=event.data.get("license_id"),
            )

    # ── xpayproxy : paiements et abonnements ──────────────────────────────────

    @event_bus.on("xpayproxy.payment.succeeded")
    async def _on_payment_succeeded(event: "Event") -> None:
        r = redis
        if not r:
            return
        amount = event.data.get("amount")
        currency = event.data.get("currency", "")
        amount_str = f"{amount} {currency}".strip() if amount else ""
        text = f"Paiement reçu{' — ' + amount_str if amount_str else ''}."
        await _notify_tenant(
            r,
            event.data.get("tenant_id"),
            "payment.succeeded",
            text,
            amount=amount,
            currency=currency,
        )

    @event_bus.on("xpayproxy.payment.failed")
    async def _on_payment_failed(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "payment.failed",
                "⚠️ Échec du paiement — veuillez mettre à jour votre moyen de paiement.",
                subscription_id=event.data.get("subscription_id"),
            )

    @event_bus.on("xpayproxy.subscription.created")
    async def _on_subscription_created(event: "Event") -> None:
        r = redis
        if r:
            plan_id = event.data.get("plan_id") or ""
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "subscription.created",
                f"Abonnement activé{' — plan ' + plan_id if plan_id else ''}.",
                plan_id=plan_id,
            )

    @event_bus.on("xpayproxy.subscription.updated")
    async def _on_subscription_updated(event: "Event") -> None:
        r = redis
        if r:
            plan_id = event.data.get("plan_id") or ""
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "subscription.updated",
                f"Abonnement mis à jour{' — plan ' + plan_id if plan_id else ''}.",
                plan_id=plan_id,
            )

    @event_bus.on("xpayproxy.subscription.cancelled")
    async def _on_subscription_cancelled(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "subscription.cancelled",
                "⚠️ Abonnement annulé — accès limité à la fin de la période en cours.",
            )

    @event_bus.on("xpayproxy.subscription.paused")
    async def _on_subscription_paused(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "subscription.paused",
                "⚠️ Abonnement suspendu.",
            )

    @event_bus.on("xpayproxy.subscription.resumed")
    async def _on_subscription_resumed(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "subscription.resumed",
                "Abonnement repris — accès rétabli.",
            )

    @event_bus.on("xpayproxy.invoice.paid")
    async def _on_invoice_paid(event: "Event") -> None:
        r = redis
        if r:
            await _notify_tenant(
                r,
                event.data.get("tenant_id"),
                "invoice.paid",
                "Facture réglée — licence renouvelée.",
                period_end=event.data.get("period_end"),
            )

    logger.info(
        "xpulse bridge enregistré — "
        "xauth (%d) + xlicense (%d) + xpayproxy (%d) events bridgés",
        10,
        6,
        8,
    )
