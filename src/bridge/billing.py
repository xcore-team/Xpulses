"""
xpulse/src/bridge/billing.py — Handlers xlicense et xpayproxy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from xcore.sdk import get_logger

if TYPE_CHECKING:
    from xcore.kernel.events import Event

    from extensions.pubsub.service import PubSubClient

logger = get_logger("xpulse.bridge")

from .handlers import _notify_tenant

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


# ── xlicense ─────────────────────────────────────────────────────────────


async def _on_license_created(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "license.created", "Licence créée pour votre tenant.",
        license_id=event.data.get("license_id"),
        plan_id=event.data.get("plan_id"), state=event.data.get("state"),
    )


async def _on_license_transitioned(event: "Event", svc: "PubSubClient") -> None:
    to_state = event.data.get("to_state", "")
    evt_type, text = _LICENSE_TRANSITION_MESSAGES.get(
        to_state, ("license.transitioned", f"Statut de licence : {to_state}.")
    )
    await _notify_tenant(
        svc, event.data.get("tenant_id"), evt_type, text,
        license_id=event.data.get("license_id"),
        from_state=event.data.get("from_state"),
        to_state=to_state, reason=event.data.get("reason"),
    )


async def _on_license_renewed(event: "Event", svc: "PubSubClient") -> None:
    expires_at = event.data.get("expires_at") or "–"
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "license.renewed", f"Licence renouvelée jusqu'au {expires_at}.",
        license_id=event.data.get("license_id"), expires_at=expires_at,
    )


async def _on_license_expired(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "license.expired",
        "⚠️ Votre licence a expiré. Renouvelez pour continuer à accéder au service.",
        license_id=event.data.get("license_id"),
    )


async def _on_license_plan_changed(event: "Event", svc: "PubSubClient") -> None:
    plan_id = event.data.get("plan_id", "")
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "license.plan_changed", f"Plan changé vers « {plan_id} ».",
        license_id=event.data.get("license_id"),
        plan_id=plan_id, reason=event.data.get("reason"),
    )


async def _on_license_key_rotated(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "license.key_rotated", "Clé de licence régénérée.",
        license_id=event.data.get("license_id"),
    )


# ── xpayproxy ────────────────────────────────────────────────────────────


async def _on_payment_succeeded(event: "Event", svc: "PubSubClient") -> None:
    amount = event.data.get("amount")
    currency = event.data.get("currency", "")
    amount_str = f"{amount} {currency}".strip() if amount else ""
    text = f"Paiement reçu{' — ' + amount_str if amount_str else ''}."
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "payment.succeeded", text,
        amount=amount, currency=currency,
    )


async def _on_payment_failed(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "payment.failed",
        "⚠️ Échec du paiement — veuillez mettre à jour votre moyen de paiement.",
        subscription_id=event.data.get("subscription_id"),
    )


async def _on_subscription_created(event: "Event", svc: "PubSubClient") -> None:
    plan_id = event.data.get("plan_id") or ""
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "subscription.created",
        f"Abonnement activé{' — plan ' + plan_id if plan_id else ''}.",
        plan_id=plan_id,
    )


async def _on_subscription_updated(event: "Event", svc: "PubSubClient") -> None:
    plan_id = event.data.get("plan_id") or ""
    await _notify_tenant(
        svc, event.data.get("tenant_id"),
        "subscription.updated",
        f"Abonnement mis à jour{' — plan ' + plan_id if plan_id else ''}.",
        plan_id=plan_id,
    )


async def _on_subscription_cancelled(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "subscription.cancelled",
        "⚠️ Abonnement annulé — accès limité à la fin de la période en cours.",
    )


async def _on_subscription_paused(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "subscription.paused",
        "⚠️ Abonnement suspendu.",
    )


async def _on_subscription_resumed(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "subscription.resumed",
        "Abonnement repris — accès rétabli.",
    )


async def _on_invoice_paid(event: "Event", svc: "PubSubClient") -> None:
    await _notify_tenant(
        svc, event.data.get("tenant_id"), "invoice.paid",
        "Facture réglée — licence renouvelée.",
        period_end=event.data.get("period_end"),
    )


HANDLERS: list[tuple[str, ...]] = [
    ("xlicense.license.created", _on_license_created, "xlicense"),
    ("xlicense.license.transitioned", _on_license_transitioned, "xlicense"),
    ("xlicense.license.renewed", _on_license_renewed, "xlicense"),
    ("xlicense.license.expired", _on_license_expired, "xlicense"),
    ("xlicense.license.plan_changed", _on_license_plan_changed, "xlicense"),
    ("xlicense.license.key_rotated", _on_license_key_rotated, "xlicense"),
    ("xpayproxy.payment.succeeded", _on_payment_succeeded, "xpayproxy"),
    ("xpayproxy.payment.failed", _on_payment_failed, "xpayproxy"),
    ("xpayproxy.subscription.created", _on_subscription_created, "xpayproxy"),
    ("xpayproxy.subscription.updated", _on_subscription_updated, "xpayproxy"),
    ("xpayproxy.subscription.cancelled", _on_subscription_cancelled, "xpayproxy"),
    ("xpayproxy.subscription.paused", _on_subscription_paused, "xpayproxy"),
    ("xpayproxy.subscription.resumed", _on_subscription_resumed, "xpayproxy"),
    ("xpayproxy.invoice.paid", _on_invoice_paid, "xpayproxy"),
]
