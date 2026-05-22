from dataclasses import dataclass


@dataclass
class RedisConfiguration:
    url: str
    channel: list[str]
    max_connection: int = 100
    MAX_CONCURRENT_STREAMS: int = 1000
    MAX_CHANNELS_PER_STREAM: int = 20
    HEARTBEAT_INTERVAL: float = 15.0
    MESSAGE_TIMEOUT: float = 0.05
    RECONNECT_MAX_RETRIES: int = 5
    RECONNECT_BASE_DELAY: float = 0.5

    @classmethod
    def _convert_str_to_list(cls, data: str | None):
        import ast

        return ast.literal_eval(data) if data else []

    @classmethod
    def from_dict(cls, data: dict):
        if not data.get("url"):
            raise ValueError(
                "Configuration manquante : 'url'du server redis  est requis dans les variables d'environnement."
            )
        if not data.get("channel"):
            raise ValueError(
                "Configuration manquante : 'channel' est requis pour ecoute."
            )

        return cls(
            url=data.get(
                "url",
            ),
            channel=cls._convert_str_to_list(data.get("channel")),
            max_connection=int(data.get("max_connection", 100)),
            MAX_CONCURRENT_STREAMS=int(data.get("MAX_CONCURRENT_STREAMS", 1000)),
            MAX_CHANNELS_PER_STREAM=int(data.get("MAX_CHANNELS_PER_STREAM", 20)),
            HEARTBEAT_INTERVAL=float(data.get("HEARTBEAT_INTERVAL", 15.0)),
            MESSAGE_TIMEOUT=float(data.get("MESSAGE_TIMEOUT", 0.05)),
            RECONNECT_MAX_RETRIES=int(data.get("RECONNECT_MAX_RETRIES", 5)),
            RECONNECT_BASE_DELAY=float(data.get("RECONNECT_BASE_DELAY", 0.5)),
        )
