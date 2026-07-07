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
