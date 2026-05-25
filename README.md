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
| `MAX_CONCURRENT_STREAMS` | Limite de connexions SSE simultanées | `1000` |
| `MAX_CHANNELS_PER_STREAM`| Max de channels par connexion client | `20` |
| `HEARTBEAT_INTERVAL` | Intervalle des pings SSE (secondes) | `15.0` |

## 📖 Utilisation API (REST/SSE)

### 1. Ouvrir un flux de notifications (SSE)
**GET** `/stream/{user_id}?channels=chan1,chan2`

```javascript
const src = new EventSource('/stream/user_123?channels=notification,alerts');

src.addEventListener('notification', (e) => {
    const data = JSON.parse(e.data);
    console.log("Message reçu:", data.text);
});
```

### 2. Publier un message
**POST** `/publish?user_id=...&text=...&channels=...`

### 3. Diffusion générale (Broadcast)
**POST** `/broadcast?text=...&channels=...`

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
