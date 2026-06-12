import asyncio
import json
import logging
import re
from typing import AsyncGenerator, Optional

from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import ConnectionError, RedisError, TimeoutError
from xcore.sdk import get_logger

from .section import RedisConfiguration

logger = get_logger("xpulse.redis")


MAX_CHANNELS_PER_STREAM = (
    20  # valeur par défaut, remplacée à l'init de RedisPubSubManager
)

# Noms de channels : lettres, chiffres, tirets, underscores, points
_CHANNEL_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,64}$")


class StreamLimitExceeded(Exception):
    pass


class InvalidChannel(ValueError):
    pass


def validate_channels(channels: list[str]) -> list[str]:
    """
    Valide et déduplique une liste de noms de channels.
    Lève InvalidChannel si un nom est invalide.
    """
    if not channels:
        raise InvalidChannel("La liste de channels est vide.")
    if len(channels) > MAX_CHANNELS_PER_STREAM:
        raise InvalidChannel(
            f"Trop de channels ({len(channels)}). Maximum autorisé : {MAX_CHANNELS_PER_STREAM}."
        )
    seen, result = set(), []
    for ch in channels:
        ch = ch.strip()
        if not _CHANNEL_RE.match(ch):
            raise InvalidChannel(
                f"Nom de channel invalide : '{ch}'. "
                "Utilisez uniquement lettres, chiffres, tirets, underscores ou points (max 64 chars)."
            )
        if ch not in seen:
            seen.add(ch)
            result.append(ch)
    return result


class RedisPubSubManager:
    """
    Wrapper Redis Pub/Sub multi-channel haute charge pour FastAPI SSE.

    Fonctionnalités :
    - Un seul stream SSE peut écouter N channels simultanément.
    - Pool de connexions partagé.
    - Compteur de streams actifs (protection mémoire).
    - Reconnexion automatique avec backoff exponentiel sur tous les channels.
    - Heartbeat SSE pour éviter les coupures proxy.
    - Chaque message SSE porte le champ `channel` pour que le client sache d'où vient l'event.
    """

    def __init__(self, config: RedisConfiguration):
        self.redis: Optional[Redis] = None
        self._pool: Optional[ConnectionPool] = None
        self._config = config
        self._active_streams = 0
        global MAX_CHANNELS_PER_STREAM

        MAX_CHANNELS_PER_STREAM = self._config.MAX_CHANNELS_PER_STREAM

    # ─────────────────────────────────────────────
    # LIFECYCLE
    # ─────────────────────────────────────────────

    async def connect(self) -> None:
        try:
            self._pool = ConnectionPool.from_url(
                self._config.url,
                decode_responses=True,
                max_connections=self._config.max_connection,
            )
            self.redis = Redis(connection_pool=self._pool)
            await self.redis.ping()
            logger.info("Redis connecté :(pool max=%d)", self._config.max_connection)
        except (ConnectionError, TimeoutError) as exc:
            logger.error("Impossible de se connecter à Redis : %s", exc)
            raise

    async def close(self) -> None:
        if self.redis:
            try:
                await self.redis.aclose()
            except RedisError as exc:
                logger.warning("Erreur à la fermeture Redis : %s", exc)
        if self._pool:
            await self._pool.aclose()
        logger.info("Redis pool fermé.")

    # ─────────────────────────────────────────────
    # PUBLISHER
    # ─────────────────────────────────────────────

    # ─────────────────────────────────────────────
    # INBOX (rétention des messages hors-ligne)
    # ─────────────────────────────────────────────

    INBOX_PREFIX = "inbox"
    INBOX_TTL = 7 * 24 * 3600  # 7 jours
    INBOX_MAX = 200  # messages max par user

    def _inbox_key(self, user_id: str) -> str:
        return f"{self.INBOX_PREFIX}:{user_id}"

    async def push_to_inbox(self, user_id: str, channel: str, event: dict) -> None:
        """Stocke un message dans la liste inbox de l'user (LPUSH + trim + TTL)."""
        if not self.redis:
            return
        key = self._inbox_key(user_id)
        entry = json.dumps({**event, "channel": channel}, ensure_ascii=False)
        try:
            await self.redis.lpush(key, entry)
            await self.redis.ltrim(key, 0, self.INBOX_MAX - 1)
            await self.redis.expire(key, self.INBOX_TTL)
        except RedisError as exc:
            logger.warning("push_to_inbox user=%s : %s", user_id, exc)

    async def flush_inbox(self, user_id: str) -> list[dict]:
        """Récupère et vide les messages en attente pour l'user."""
        if not self.redis:
            return []
        key = self._inbox_key(user_id)
        try:
            raw_messages = await self.redis.lrange(key, 0, -1)
            if raw_messages:
                await self.redis.delete(key)
            # LPUSH = newest first, on inverse pour ordre chronologique
            result = []
            for raw in reversed(raw_messages):
                try:
                    result.append(json.loads(raw))
                except json.JSONDecodeError:
                    pass
            return result
        except RedisError as exc:
            logger.warning("flush_inbox user=%s : %s", user_id, exc)
            return []

    async def inbox_count(self, user_id: str) -> int:
        """Retourne le nombre de messages en attente."""
        if not self.redis:
            return 0
        try:
            return await self.redis.llen(self._inbox_key(user_id))
        except RedisError:
            return 0

    # ─────────────────────────────────────────────
    # PUBLISHER
    # ─────────────────────────────────────────────

    async def publish(self, channel: str, event: dict) -> bool:
        """
        Publie un event sur un channel.
        Si le message cible un user_id ET qu'aucun abonné n'est actif → stocké en inbox.
        """
        if not self.redis:
            logger.error("publish() : Redis non initialisé.")
            return False
        try:
            payload = json.dumps(event, ensure_ascii=False)
            receivers = await self.redis.publish(channel, payload)
            logger.debug("Publié sur '%s' → %d abonné(s)", channel, receivers)

            # Aucun abonné actif et message ciblé → inbox
            user_id = event.get("user_id")
            if receivers == 0 and user_id:
                await self.push_to_inbox(user_id, channel, event)
                logger.debug("Message mis en inbox pour user=%s (0 abonnés)", user_id)

            return True
        except (ConnectionError, TimeoutError, RedisError) as exc:
            logger.error("Erreur publish sur '%s' : %s", channel, exc)
            return False

    async def publish_many(self, channels: list[str], event: dict) -> dict[str, bool]:
        """
        Publie le même event sur plusieurs channels en parallèle.
        Retourne un dict {channel: succès}.
        """
        results = await asyncio.gather(
            *[self.publish(ch, event) for ch in channels],
            return_exceptions=False,
        )
        return dict(zip(channels, results))

    # ─────────────────────────────────────────────
    # STREAM SSE MULTI-CHANNEL
    # ─────────────────────────────────────────────

    async def stream(
        self,
        channels: list[str],
        user_id: Optional[str] = None,
        filter_key: str = "user_id",
    ) -> AsyncGenerator[str, None]:
        """
        Générateur SSE multi-channel compatible StreamingResponse.

        - Chaque message SSE inclut `channel` pour que le client sache
          d'où vient l'event.
        - Le type SSE `event:` est positionné sur le nom du channel,
          ce qui permet au client JS de filtrer par addEventListener.
        - Le filtrage user_id s'applique sur tous les channels.

        Exemple de payload reçu côté client :
            event: notification
            data: {"channel": "notification", "user_id": "123", "text": "Bonjour"}

        Usage JS :
            const src = new EventSource('/stream/user123?channels=notification,alerts');
            src.addEventListener('notification', e => console.log(JSON.parse(e.data)));
            src.addEventListener('alerts',       e => console.log(JSON.parse(e.data)));
            src.addEventListener('error',        e => console.error(e));
        """
        if self._active_streams >= self._config.MAX_CONCURRENT_STREAMS:
            logger.warning(
                "Limite de streams atteinte (%d). Refus user=%s",
                self._config.MAX_CONCURRENT_STREAMS,
                user_id,
            )
            raise StreamLimitExceeded(
                f"Trop de connexions simultanées ({self._config.MAX_CONCURRENT_STREAMS} max)."
            )

        self._active_streams += 1
        logger.info(
            "Stream ouvert user=%s channels=%s [actifs: %d]",
            user_id,
            channels,
            self._active_streams,
        )

        pubsub = self.redis.pubsub()
        last_heartbeat = asyncio.get_event_loop().time()

        try:
            await pubsub.subscribe(*channels)

            while True:
                now = asyncio.get_event_loop().time()

                # ── Heartbeat ─────────────────────────────────────────────
                if now - last_heartbeat >= self._config.HEARTBEAT_INTERVAL:
                    yield ": ping\n\n"
                    last_heartbeat = now

                # ── Lecture ───────────────────────────────────────────────
                try:
                    msg = await asyncio.wait_for(
                        pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=self._config.MESSAGE_TIMEOUT,
                        ),
                        timeout=self._config.MESSAGE_TIMEOUT + 1.0,
                    )
                except asyncio.TimeoutError:
                    await asyncio.sleep(0)
                    continue
                except (ConnectionError, TimeoutError) as exc:
                    logger.warning(
                        "Redis perdu (user=%s) : %s — reconnexion…", user_id, exc
                    )
                    reconnected = await self._reconnect_pubsub(pubsub, channels)
                    if not reconnected:
                        logger.error(
                            "Reconnexion impossible. Fermeture stream user=%s.", user_id
                        )
                        yield f"event: error\ndata: {json.dumps({'error': 'redis_unavailable'})}\n\n"
                        break
                    continue

                if msg and msg.get("type") == "message":
                    source_channel = msg.get("channel", "unknown")
                    try:
                        event = json.loads(msg["data"])
                    except (json.JSONDecodeError, KeyError) as exc:
                        logger.warning(
                            "Message malformé ignoré (channel=%s) : %s",
                            source_channel,
                            exc,
                        )
                        continue

                    # Filtrage : si le message a un user_id, on ne le délivre qu'au bon user.
                    # Si le message n'a pas de user_id (broadcast), on le délivre à tous les abonnés.
                    event_user_id = event.get(filter_key)
                    if (
                        user_id is not None
                        and event_user_id is not None
                        and event_user_id != user_id
                    ):
                        continue

                    # On injecte le nom du channel dans le payload
                    event["channel"] = source_channel

                    # SSE : event type = nom du channel → le client peut filtrer avec addEventListener
                    yield (
                        f"event: {source_channel}\n"
                        f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                    )
                else:
                    await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.info("Stream annulé par le client user=%s.", user_id)
        except Exception as exc:
            logger.exception("Erreur inattendue stream user=%s : %s", user_id, exc)
            yield f"event: error\ndata: {json.dumps({'error': 'internal_error'})}\n\n"
        finally:
            self._active_streams -= 1
            logger.info(
                "Stream fermé user=%s channels=%s [actifs: %d]",
                user_id,
                channels,
                self._active_streams,
            )
            try:
                await pubsub.unsubscribe(*channels)
                await pubsub.aclose()
            except RedisError as exc:
                logger.warning("Erreur cleanup pubsub : %s", exc)

    # ─────────────────────────────────────────────
    # RECONNEXION
    # ─────────────────────────────────────────────

    async def _reconnect_pubsub(self, pubsub, channels: list[str]) -> bool:
        delay = self._config.RECONNECT_BASE_DELAY
        for attempt in range(1, self._config.RECONNECT_MAX_RETRIES + 1):
            await asyncio.sleep(delay)
            try:
                await pubsub.subscribe(*channels)
                logger.info(
                    "Reconnexion réussie (channels=%s, tentative %d).",
                    channels,
                    attempt,
                )
                return True
            except RedisError as exc:
                logger.warning(
                    "Reconnexion %d/%d échouée : %s",
                    attempt,
                    self._config.RECONNECT_MAX_RETRIES,
                    exc,
                )
                delay = min(delay * 2, 30.0)
        return False

    # ─────────────────────────────────────────────
    # MÉTRIQUES
    # ─────────────────────────────────────────────

    @property
    def active_streams(self) -> int:
        return self._active_streams

    async def health_check(self) -> bool:
        try:
            return await self.redis.ping()
        except RedisError:
            return False
