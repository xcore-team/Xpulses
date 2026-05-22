# XPulse — Guide d'intégration

XPulse est le plugin de notifications temps-réel de la plateforme **Xcore**. Il expose un flux **SSE (Server-Sent Events)** multi-channel alimenté par Redis Pub/Sub, et permet à n'importe quel autre plugin d'envoyer des notifications ciblées ou des broadcasts via le bus d'événements ou les actions IPC.

---

## Sommaire

1. [Installation et configuration](#1-installation-et-configuration)
2. [Variables d'environnement](#2-variables-denvironnement)
3. [Endpoints HTTP](#3-endpoints-http)
4. [Utilisation depuis un autre plugin Xcore](#4-utilisation-depuis-un-autre-plugin-xcore)
5. [Bus d'événements](#5-bus-dévénements)
6. [Actions IPC](#6-actions-ipc)
7. [Connexion SSE côté client](#7-connexion-sse-côté-client)
8. [Channels — nommage et validation](#8-channels--nommage-et-validation)
9. [Limites et configuration avancée](#9-limites-et-configuration-avancée)
10. [Sécurité et signatures](#10-sécurité-et-signatures)
11. [Health check](#11-health-check)

---

## 1. Installation et configuration

Déclarez XPulse dans votre hub Xcore :

```yaml
# hub/plugins.yaml
plugins:
  - path: app/XPulse
```

XPulse nécessite un serveur **Redis** accessible avant son chargement. La connexion est établie au `on_load` via un pool partagé. Le plugin démarre en mode dégradé si Redis est indisponible.

---

## 2. Variables d'environnement

Copiez `example.env` en `.env` à la racine du plugin :

```bash
cp app/XPulse/example.env app/XPulse/.env
```

### Variables obligatoires

| Variable | Description | Exemple |
|----------|-------------|---------|
| `URL` | URL de connexion Redis | `redis://localhost:6379/0` |
| `channel` | Channels écoutés (liste Python) | `["notification", "alerts"]` |

### Variables optionnelles

| Variable | Défaut | Description |
|----------|--------|-------------|
| `MAX_CONCURRENT_STREAMS` | `1000` | Nombre max de connexions SSE simultanées |
| `MAX_CHANNELS_PER_STREAM` | `20` | Nombre max de channels par stream SSE |
| `HEARTBEAT_INTERVAL` | `15.0` | Intervalle (s) du ping SSE pour maintenir la connexion |
| `MESSAGE_TIMEOUT` | `0.05` | Délai (s) de polling Redis par itération |
| `RECONNECT_MAX_RETRIES` | `5` | Tentatives de reconnexion Redis en cas de coupure |
| `RECONNECT_BASE_DELAY` | `0.5` | Délai initial (s) entre les tentatives (backoff exponentiel) |

### Exemple de `.env`

```dotenv
URL=redis://localhost:6379/0
channel=["notification", "alerts", "system"]
MAX_CONCURRENT_STREAMS=500
MAX_CHANNELS_PER_STREAM=10
HEARTBEAT_INTERVAL=20.0
MESSAGE_TIMEOUT=0.05
RECONNECT_MAX_RETRIES=5
RECONNECT_BASE_DELAY=1.0
```

---

## 3. Endpoints HTTP

Tous les endpoints sont montés sous le préfixe du plugin (ex. `/app/xpulse`).

### `GET /stream`

Ouvre un flux SSE pour l'utilisateur authentifié.

| Paramètre | Type | Requis | Description |
|-----------|------|--------|-------------|
| `channels` | `list[str]` | non | Channels à écouter (défaut : `["notification"]`) |

**Authentification requise** — le token JWT est vérifié via `get_current_user`.

```
GET /app/xpulse/stream?channels=notification&channels=alerts
Authorization: Bearer <token>
```

**Format des événements SSE reçus :**
```
event: notification
data: {"channel": "notification", "user_id": "abc123", "text": "Bonjour"}

: ping
```

---

### `POST /publish`

Publie un message ciblé vers un utilisateur précis.

| Paramètre query | Type | Requis | Description |
|-----------------|------|--------|-------------|
| `user_id` | `str` | oui | ID de l'utilisateur destinataire |
| `text` | `str` | oui | Message à envoyer |
| `channels` | `list[str]` | non | Channels cibles (défaut : `["notification"]`) |

**Permission requise** : `xpulse:publish`

---

### `POST /broadcast`

Envoie un message à **tous** les abonnés d'un ou plusieurs channels.

| Paramètre query | Type | Requis | Description |
|-----------------|------|--------|-------------|
| `text` | `str` | oui | Message à broadcaster |
| `channels` | `list[str]` | non | Channels cibles (défaut : `["notification"]`) |

**Permission requise** : `xpulse:broadcast`

---

## 4. Utilisation depuis un autre plugin Xcore

### Via le bus d'événements (recommandé)

```python
# Notification ciblée vers un utilisateur
await ctx.events.emit("ext.notification.publish", {
    "channels": ["notification"],
    "user_id": "abc123",
    "event": "ORDER_SHIPPED",
    "order_id": "order-456",
})

# Broadcast à tous les abonnés
await ctx.events.emit("ext.notification.broadcast", {
    "channels": ["system"],
    "event": "MAINTENANCE_SCHEDULED",
    "message": "Maintenance prévue à 22h00.",
})
```

### Via les actions IPC

```python
# Notification ciblée
result = await ctx.actions.call("xpulse.publish", {
    "user_id": "abc123",
    "text": "Votre commande a été expédiée.",
    "channels": ["notification"],
})

# Broadcast
result = await ctx.actions.call("xpulse.broadcast", {
    "text": "Mise à jour système disponible.",
    "channels": ["system", "alerts"],
})

# Compter les streams actifs sur un channel
result = await ctx.actions.call("xpulse.subscribers", {
    "channel": "notification",
})
# result["active_streams"] → nombre de connexions SSE ouvertes
```

---

## 5. Bus d'événements

### Événements consommés

| Événement | Description | Payload |
|-----------|-------------|---------|
| `ext.notification.publish` | Publie un message pour un utilisateur précis | `{ "user_id": "...", "channels": [...], ...données }` |
| `ext.notification.broadcast` | Diffuse à tous les abonnés | `{ "channels": [...], ...données }` |

> Le payload complet de l'événement est transmis tel quel dans le flux SSE. Vous pouvez y inclure n'importe quel champ (`event`, `submission_id`, `order_id`, etc.) — ils seront visibles côté client.

### Distinction publish / broadcast

| Comportement | Condition |
|---|---|
| Notification ciblée | Le message contient `user_id` → livré uniquement à ce user |
| Broadcast | Le message ne contient pas `user_id` → livré à tous les abonnés du channel |

---

## 6. Actions IPC

| Action | Description | Payload requis |
|--------|-------------|----------------|
| `xpulse.publish` | Notification ciblée | `user_id`, `text`, `channels` |
| `xpulse.broadcast` | Broadcast tous abonnés | `text`, `channels` |
| `xpulse.stream` | Injecte un event dans Redis | `user_id`, `channels` |
| `xpulse.subscribers` | Nombre de streams actifs | `channel` |
| `xpulse.email` | Envoie un email via `ext.email` | `to`, `subject`, `template`, `html_parser` |

---

## 7. Connexion SSE côté client

### JavaScript (EventSource)

```javascript
// Note : EventSource ne supporte pas les headers Authorization natifs.
// Utilisez un cookie de session ou un proxy backend qui injecte le token.

const source = new EventSource(
    "/app/xpulse/stream?channels=notification&channels=alerts",
    { withCredentials: true }
);

// Écouter un channel spécifique
source.addEventListener("notification", (e) => {
    const data = JSON.parse(e.data);
    console.log("Notification reçue :", data);
});

source.addEventListener("alerts", (e) => {
    const data = JSON.parse(e.data);
    console.log("Alerte :", data);
});

source.addEventListener("error", (e) => {
    console.error("Erreur SSE :", e);
});

// source.close(); // Fermer le stream
```

### Python (httpx)

```python
import httpx, json

async with httpx.AsyncClient() as client:
    async with client.stream(
        "GET",
        "http://localhost:8000/app/xpulse/stream",
        params={"channels": ["notification"]},
        headers={"Authorization": f"Bearer {token}"},
    ) as response:
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                data = json.loads(line[5:].strip())
                print("Reçu :", data)
```

---

## 8. Channels — nommage et validation

Un nom de channel valide :
- Contient uniquement des **lettres, chiffres, tirets (`-`), underscores (`_`) ou points (`.`)**
- Fait entre **1 et 64 caractères**

**Channels recommandés :**

| Channel | Usage |
|---------|-------|
| `notification` | Notifications utilisateur générales |
| `alerts` | Alertes système et avertissements |
| `system` | Messages système internes |
| `admin` | Flux réservé aux administrateurs |
| `broadcast` | Diffusion globale à tous les utilisateurs |

---

## 9. Limites et configuration avancée

| Limite | Défaut | Variable |
|--------|--------|----------|
| Streams simultanés | 1 000 | `MAX_CONCURRENT_STREAMS` |
| Channels par stream | 20 | `MAX_CHANNELS_PER_STREAM` |

Lorsque `MAX_CONCURRENT_STREAMS` est atteint, tout nouveau `GET /stream` reçoit une erreur `503 Service Unavailable`.

### Reconnexion automatique

En cas de coupure Redis, XPulse tente de se reconnecter avec un **backoff exponentiel** :

```
tentative 1 → délai = RECONNECT_BASE_DELAY
tentative 2 → délai × 2
...
tentative N → délai plafonné à 30s
```

Si toutes les tentatives échouent, le stream SSE envoie `event: error` au client puis se ferme proprement.

---

## 10. Sécurité et signatures

XPulse fonctionne en mode `trusted` — il possède un fichier `plugin.sig`. Toute modification du code source dans `src/` invalide la signature. Re-signez après chaque modification :

```bash
xcore plugin sign ./XPulse
```

---

## 11. Health check

XPulse enregistre automatiquement un health check Redis sous la clé `xpulse.redis` :

```json
// Redis opérationnel
{"xpulse.redis": {"healthy": true, "message": "Redis répond."}}

// Redis indisponible
{"xpulse.redis": {"healthy": false, "message": "Redis ne répond pas."}}
```

---

## Dépendances inter-plugins

| Plugin | Obligatoire | Raison |
|--------|-------------|--------|
| `auth` (xauth) | Non (implicite) | `get_current_user` / `require_permission` supposent un AuthBackend enregistré |

XPulse est autonome — il n'a pas de dépendance déclarée dans `plugin.yaml`. Il fonctionne dès qu'un AuthBackend est actif dans le hub (typiquement **xauth**).
