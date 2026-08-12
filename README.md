# XPulse

XPulse est un système de notification en temps réel haute performance conçu comme une extension pour le framework **XCore**. Il permet de diffuser des messages aux utilisateurs via **Server-Sent Events (SSE)** en s'appuyant sur **Redis Pub/Sub** pour la distribution des messages à grande échelle.

## 🚀 Fonctionnalités

- **Streaming SSE Multi-channel** : Un client peut s'abonner à plusieurs flux de notifications via une seule connexion.
- **Filtrage Multi-tenant** : Les messages sont automatiquement filtrés par `user_id`.
- **Heartbeat & Résilience** : Gestion intégrée des pings pour maintenir les connexions actives et reconnexion automatique à Redis avec backoff exponentiel.
- **Intégration Native XCore** : Support complet du bus d'événements et des actions XCore.
- **Monitoring** : Route de health-check pour surveiller l'état de la connexion Redis et le nombre de flux actifs.

## 🛠️ Configuration

Le plugin se configure via le fichier `plugin.yaml` ou les variables d'environnement suivantes :

| Variable | Description | Défaut |
|----------|-------------|---------|
| `url` | URL de connexion Redis | `redis://localhost:6379/0` |
| `channel` | Channels par défaut à écouter | `['notification', 'systeme', 'hunters']` |
| `MAX_CONCURRENT_STREAMS` *(non implémenté)* | Limite de connexions SSE simultanées | — |
| `MAX_CHANNELS_PER_STREAM` *(non implémenté)* | Max de channels par connexion client — `utils.validate_channels` applique déjà un plafond fixe de 20, non configurable | `20` (codé en dur) |
| `HEARTBEAT_INTERVAL` *(non implémenté)* | Intervalle des pings SSE (secondes) — configurable via `RedisConfig.heartbeat`/`plugin.yaml`, pas par cette variable | — |

> Ces variables (ainsi que `MESSAGE_TIMEOUT`, `RECONNECT_MAX_RETRIES`, `RECONNECT_BASE_DELAY`) décrivent un plan de configuration jamais câblé : `plugin.yaml` déclare `envconfiguration.inject: false` et aucune n'est lue dans `app/XPulses/src`. À implémenter ou retirer de la documentation.

## 📖 Utilisation API (REST/SSE)

### 1. Ouvrir un flux de notifications (SSE)
**GET** `/stream?channels=chan1,chan2` — l'utilisateur est dérivé du JWT (`Authorization: Bearer ...`), pas d'un path param.

```javascript
const src = new EventSource('/stream?channels=notification,alerts', {
    headers: { Authorization: `Bearer ${token}` },
});

src.addEventListener('notification', (e) => {
    const data = JSON.parse(e.data);
    console.log("Message reçu:", data.text);
});
```

### 2. Publier un message
**POST** `/publish` — corps JSON `{"user_id": "...", "text": "...", "channels": [...]}`

### 3. Diffusion générale (Broadcast)
**POST** `/broadcast` — corps JSON `{"text": "...", "channels": [...]}`

---

## 🏗️ Développement

### Prérequis
- Python 3.10+
- Un serveur Redis actif
- Le Kernel XCore installé

### Installation locale
1. Clonez le dépôt dans le dossier `plugins/` de votre instance XCore.
2. Assurez-vous que les dépendances sont satisfaites (nécessite le plugin `auth`).
3. Lancez XCore.
