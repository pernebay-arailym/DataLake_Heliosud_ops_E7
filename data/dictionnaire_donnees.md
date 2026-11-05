# Dictionnaire de données — HélioSud (épreuve E7)

Ce dictionnaire décrit les données **telles que documentées par l'équipe d'HélioSud**. Il peut être incomplet ou inexact : c'est à vous de le vérifier et de le compléter lors de l'analyse (tâche 3.1) puis dans le catalogue (tâche 3.6).

Toutes les données sont **synthétiques** : générées aléatoirement, elles ne correspondent à aucune personne ni installation réelle.

## Contenu du dossier `data/`

| Élément | Mode d'alimentation | Contenu |
|---|---|---|
| `batch/referentiel_sites.csv` | Batch (ponctuel) | 40 sites de production |
| `batch/referentiel_onduleurs.csv` | Batch (ponctuel) | Parc d'onduleurs (≈ 8 100) |
| `batch/contrats_foyers.csv` | Batch (quotidien) | Contrats des foyers (données personnelles) |
| `batch/courbes_charge/AAAA/MM/` | Batch (quotidien) | Un fichier CSV par jour |
| `batch/meteo/AAAA/MM/JJ/` | Micro-batch (horaire) | Un fichier par site et par heure |
| `batch/rapports/AAAA/MM/` | Batch (à l'arrivée) | PDF, image thermographique, métadonnées JSON |
| Flux onduleurs | Temps réel | Produit par le simulateur (voir plus bas) |
| `telemetrie_exemple_1h.jsonl` | — | Une heure de flux, pour découvrir les schémas sans lancer le simulateur |

Le jeu batch fourni est un **échantillon de test** : 7 jours (du 2 au 8 juin 2025), 1 200 foyers, environ 270 rapports. Il représente une fraction des volumes de production indiqués dans le brief.

## Référentiels

### referentiel_sites.csv
| Colonne | Description |
|---|---|
| site_id | Identifiant du site (SITE-01 à SITE-40) |
| nom, commune | Nom de la centrale et commune |
| type_installation | Sol ou Toiture |
| puissance_mwc | Puissance crête en MWc |
| latitude, longitude | Coordonnées GPS approximatives |
| constructeur_onduleur | Constructeur des onduleurs du site |
| date_mise_en_service | Date de mise en service |
| nb_onduleurs | Nombre d'onduleurs |

### referentiel_onduleurs.csv
| Colonne | Description |
|---|---|
| onduleur_id | Identifiant (`SITE-xx-INV-nnn`) |
| site_id | Site d'appartenance |
| constructeur, modele | Constructeur et modèle |
| puissance_nominale_kw | Puissance nominale en kW |
| date_installation | Date d'installation |

## Contrats des foyers (`contrats_foyers.csv`, CSV, données personnelles)
| Colonne | Description |
|---|---|
| contrat_id | Identifiant du contrat |
| pdl | Point de livraison du compteur communicant (14 chiffres) |
| nom, prenom, email, telephone, adresse, code_postal, commune | Identité et coordonnées du titulaire |
| puissance_crete_kwc | Puissance crête de l'installation du foyer |
| date_souscription | Date de souscription |
| tarif | Option tarifaire |
| statut | Statut du contrat |

## Courbes de charge (`courbes_charge_AAAAMMJJ.csv`)

Une ligne par point de livraison et par pas de 30 minutes : le point de livraison, l'horodatage, l'énergie consommée et l'énergie produite sur le pas. Le fichier d'un jour contient les 48 pas de la journée. Un identifiant de point de livraison permet de retrouver le contrat dans `contrats_foyers.csv`.

**Ces données sont personnelles** : à ce pas de temps, elles révèlent les habitudes de vie du foyer.

## Météo (`meteo_<site>_<AAAAMMJJHH>.csv` ou `.json`)

Un relevé horaire par site : rayonnement global (W/m²), nébulosité, température. Deux fournisseurs météo alimentent le dossier, avec des formats différents.

## Rapports d'intervention (`rapports/`)

Pour chaque intervention d'un technicien :
- `rapport_<id>.pdf` : rapport rédigé (site, onduleur, date, constat, température maximale relevée) ;
- `rapport_<id>_thermo.jpg` : photo thermographique de l'onduleur ;
- `rapport_<id>.json` : métadonnées (site, onduleur, date, type d'intervention, durée, fichiers associés).

Le technicien est identifié par un code (`TECH-nn`), sans nom.

## Flux temps réel : télémétrie des onduleurs

Chaque onduleur émet un message JSON par minute. Le format dépend du constructeur.

**SolarTech** (sites impairs)
```json
{"device_id": "SITE-01-INV-001", "ts": "2025-06-02T10:00:00Z", "p_ac_w": 28815, "v_dc": 723.5, "temp_c": 34.8, "status_code": 0}
```

**Voltaris** (sites pairs)
```json
{"inverter": {"id": "SITE-02-INV-001", "model": "VX-60"}, "time": "2025-06-02T12:00:00+02:00", "power_kw": 33.87, "dc_voltage_v": 719.4, "temperature": 34.9, "fault": null}
```

Codes de défaut connus (liste fournie par les constructeurs, possiblement incomplète) :

| Constructeur | Code | Signification |
|---|---|---|
| SolarTech | 0 | Production normale |
| SolarTech | 1 | Veille |
| SolarTech | 17 | Surchauffe |
| SolarTech | 42 | Défaut d'isolement |
| Voltaris | `null` | Fonctionnement normal |
| Voltaris | E104 | Surchauffe |

### Lancer le simulateur

Le simulateur est dans `generation/simulateur_onduleurs.py` (Python 3, `numpy` ; `kafka-python` pour l'envoi vers Kafka). Une minute simulée correspond à un « tick ».

```bash
cd generation
# Aperçu à l'écran (5 minutes simulées)
python3 simulateur_onduleurs.py --minutes 5 --intervalle 0

# Un jour d'historique dans un fichier
python3 simulateur_onduleurs.py --minutes 1440 --intervalle 0 --sink fichier --fichier onduleurs_jour1.jsonl

# Flux continu vers Kafka (1 minute simulée par seconde réelle)
python3 simulateur_onduleurs.py --debut maintenant --sink kafka --bootstrap localhost:9092 --topic onduleurs.telemetrie

# Flotte complète (≈ 8 100 onduleurs, ≈ 12 millions de messages par jour simulé), en temps réel
python3 simulateur_onduleurs.py --sites 40 --onduleurs-par-site 0 --intervalle 60 --sink kafka

# Panne rapide d'un onduleur, pour tester les alertes
python3 simulateur_onduleurs.py --demo-panne --intervalle 1
```

Principales options : `--intervalle` (secondes réelles entre deux ticks, 60 = temps réel), `--sites`, `--onduleurs-par-site`, `--taux-trous`, `--taux-doublons`, `--seed`. Voir `--help`.

**Dates et vitesse du flux**
- Sans option `--debut`, les messages sont datés à partir du 2 juin 2025 : ce sont des données « anciennes » par rapport à la date du jour, ce qui compte pour le partitionnement par date et pour les règles de suppression ou d'archivage.
- Avec `--debut maintenant`, les messages partent de l'heure réelle. Mais avec `--intervalle 1` (valeur par défaut), le temps simulé avance 60 fois plus vite que le temps réel : après une heure d'exécution, les horodatages ont 59 heures d'avance sur l'heure réelle. Utilisez `--intervalle 60` pour un flux en temps réel.
- Le simulateur tourne tant qu'on ne l'arrête pas (Ctrl+C) : lancez-le dans un terminal dédié, sans oublier de l'arrêter à la fin.
- Le simulateur a une cadence maximale : environ 2 600 messages par seconde sur la machine de test, vers un broker Kafka local. Avec la flotte complète (≈ 8 100 messages par minute simulée), la vitesse par défaut (`--intervalle 1`, soit ≈ 8 100 messages par seconde) n'est donc pas tenue : utilisez `--intervalle 60` ou réduisez le nombre d'onduleurs.
