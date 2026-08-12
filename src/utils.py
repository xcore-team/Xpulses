from __future__ import annotations

import re
from typing import TYPE_CHECKING

from fastapi import HTTPException, status

if TYPE_CHECKING:
    from extensions.pubsub.service import PubSubClient

_CHANNEL_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,64}$")


class InvalidChannel(ValueError):
    pass


def validate_channels(channels: list[str], max_channels: int = 20) -> list[str]:
    if not channels:
        raise InvalidChannel("La liste de channels est vide.")
    if len(channels) > max_channels:
        raise InvalidChannel(
            f"Trop de channels ({len(channels)}). Maximum autorisé : {max_channels}."
        )
    seen, result = set(), []
    for ch in channels:
        ch = ch.strip()
        if not _CHANNEL_RE.match(ch):
            raise InvalidChannel(
                f"Nom de channel invalide : '{ch}'. "
                "Utilisez uniquement lettres, chiffres, tirets, "
                "underscores ou points (max 64 chars)."
            )
        if ch not in seen:
            seen.add(ch)
            result.append(ch)
    return result


def parse_channels(raw: list[str], max_channels: int = 20) -> list[str]:
    flat = []
    for c in raw:
        flat.extend(c.split(","))
    try:
        return validate_channels(flat, max_channels)
    except InvalidChannel as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        )


def require_pubsub(svc: "PubSubClient | None") -> "PubSubClient":
    if not svc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service Redis indisponible.",
        )
    return svc


# Canaux de diffusion globale : n'importe quel utilisateur authentifié peut
# s'y abonner (le contenu n'est jamais personnel par construction — cf.
# _notify_tenant/broadcast qui ne posent jamais `user_id` dans le payload).
_GLOBAL_BROADCAST_CHANNELS = {"system", "broadcast"}
_TENANT_CHANNEL_PREFIX = "tenant-"


def is_broadcast_channel(channel: str) -> bool:
    """Un canal de diffusion (pas de filtrage par user_id côté stream) —
    soit global (`system`/`broadcast`), soit un canal tenant (`tenant-<id>`,
    audit XPulse Constat 2 : ces messages ne portent jamais de `user_id`,
    seulement `tenant_id`, donc le filtre `event.get("user_id") == user_id`
    les bloquait systématiquement quel que soit l'abonné)."""
    return channel in _GLOBAL_BROADCAST_CHANNELS or channel.startswith(_TENANT_CHANNEL_PREFIX)


def _extract_user_tenant_id(user: dict) -> str | None:
    return user.get("tenant_id") or user.get("user", {}).get("tenant_id")


def authorize_channels(channels: list[str], user: dict) -> None:
    """Autorisation par canal pour `/stream` (audit XPulse Constat 3).

    Un canal `tenant-<id>` n'est écoutable que par un membre de ce tenant
    (ou un porteur d'un grant `xpulse:admin:*`) — sans quoi lever le filtre
    `user_id` du Constat 2 sur ces canaux ouvrirait une fuite cross-tenant
    immédiate. Les canaux globaux (`system`/`broadcast`) et les canaux
    personnels (filtrés par user_id en aval) restent ouverts à tout
    utilisateur authentifié.
    """
    granted = set(user.get("roles", [])) | set(user.get("permissions", []))
    is_admin = bool(granted & {"xpulse:admin:publish", "xpulse:admin:broadcast"})
    own_tenant_id = _extract_user_tenant_id(user)

    for channel in channels:
        if channel.startswith(_TENANT_CHANNEL_PREFIX):
            target_tenant_id = channel[len(_TENANT_CHANNEL_PREFIX):]
            if is_admin:
                continue
            if not own_tenant_id or target_tenant_id != own_tenant_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Non autorisé à écouter le canal '{channel}'.",
                )
