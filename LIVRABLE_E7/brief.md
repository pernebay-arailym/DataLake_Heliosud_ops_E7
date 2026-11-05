# Description

Vous êtes data engineer junior au sein de DataFlow Conseil, une ESN spécialisée dans les solutions data pour le secteur de l'énergie. Votre client, la société HélioSud, exploite 40 centrales photovoltaïques (au sol et en toiture) dans le sud de la France et supervise 12 000 installations de particuliers en autoconsommation.

Chaque centrale compte en moyenne 200 onduleurs qui remontent leurs mesures toutes les minutes. Les compteurs communicants des foyers transmettent chaque jour leurs courbes de consommation et de production. S'y ajoutent les relevés météo, les rapports d'intervention des techniciens (PDF) et leurs photos thermographiques. Ces données sont aujourd'hui dispersées entre des serveurs de fichiers et des bases locales : elles ne sont ni structurées, ni documentées, ni protégées par une gestion cohérente des droits, et leur volume croît de façon continue.

HélioSud vous confie la mission de **mettre en conditions opérationnelles un data lake**, de la conception de son architecture à la gestion de tous les outils qui permettent son usage : stockage, alimentation batch et temps réel, catalogue, monitorage et gouvernance des accès. Ce data lake doit servir de fondation à deux projets futurs : la détection précoce des défaillances d'onduleurs (maintenance prédictive) et la prévision de la production d'énergie.



## Contexte du projet

### 2. Contexte et cadre technique

Le data lake est à construire dans un environnement de développement/test. Le cadre d'exploitation est le suivant :

- Infrastructure : une machine virtuelle unique (Linux / WSL2) ou un hôte Docker, sans service cloud managé
- Déploiement : conteneurs (Docker Compose), pour que toute l'installation soit reproductible
- Versioning : Git
- Langage de scripts : Python (`boto3`, `kafka-python` ou équivalent)
- Contrainte budgétaire : solutions open source uniquement

Le choix des technologies du data lake fait partie de l'exercice : vous devez le justifier au regard de la volumétrie, de la variété et de la vitesse des données. À titre de repère, les familles d'outils attendues sont les suivantes (les exemples ne sont pas imposés) :

| Composant                     | Exemples d'outils                                              |
|-------------------------------|----------------------------------------------------------------|
| Système de stockage           | SeaweedFS, RustFS, Ceph, HDFS                                  |
| Outil batch                   | Apache Airflow, Apache Spark, scripts Python planifiés         |
| Outil temps réel              | Apache Kafka, Redpanda, RabbitMQ (ou broker MQTT en amont)     |
| Catalogue de données          | OpenMetadata, Apache Atlas                                     |
| Monitorage et alerte          | Prometheus + Grafana + Alertmanager, Zabbix                    |

Les jeux de données vous sont fournis dans le dossier `data/` du drive, avec leur dictionnaire de données : des fichiers pour les sources batch et un extrait d'une heure du flux temps réel. Le flux temps réel lui-même est produit par un simulateur Python (`generation/simulateur_onduleurs.py`), fourni dans le dossier `generation/` du drive.

Les courbes de consommation des foyers et leurs contrats sont des données personnelles : à un pas de temps fin, elles permettent de déduire des habitudes de vie. L'architecture, le cycle de vie des données et les accès doivent respecter le RGPD.

À terme, le data lake alimentera les outils de reporting de HélioSud (suivi de production, déclarations réglementaires) et les projets de data science. Il ne les remplace pas.


## Modalités pédagogiques
### 3. Tâches à réaliser

Votre rapport professionnel devra couvrir l'ensemble des tâches suivantes. Chaque tâche doit être documentée avec la méthodologie employée, les commandes exécutées, les résultats obtenus et les éventuelles difficultés rencontrées.

#### 3.1 Analyse des données et choix techniques (C18)
- Analyser chaque source de la section 4 selon les trois critères volume, vitesse et variété, et en déduire les contraintes d'architecture
- Proposer des choix techniques en fonction des contraintes IT du cadre d'exploitation, des problématiques visées et de la nature des données
- Comparer au moins deux solutions pour le système de stockage, l'outil batch et l'outil temps réel, et justifier la solution retenue pour chacun
- Vérifier que les propositions sont cohérentes avec le cadre d'exploitation (ressources de la machine, absence de cloud, budget)

#### 3.2 Architecture du data lake (C18)
- Produire le schéma d'architecture technique du data lake, lisible et réalisé avec un formalisme approprié (ex : modèle C4, ArchiMate, ou schéma légendé et normalisé)
- Organiser le stockage en zones ou en couches (ex : `raw`, `staging`, `curated`, `archive`) et définir la convention de nommage et de partitionnement (ex : `source/centrale/année/mois/jour`)
- Représenter les flux batch et temps réel, du producteur de la donnée à son consommateur
- Montrer comment l'architecture tient compte des contraintes de volume, de vitesse et de variété

#### 3.3 Comparaison et choix du catalogue de données (C18)
- Comparer au moins deux outils de catalogue de données (ex : OpenMetadata, Apache Atlas) selon des critères définis : fonctionnalités, connecteurs vers le stockage choisi, ressources nécessaires, gestion des droits d'accès, exploitabilité, maturité de la communauté
- Justifier le choix du catalogue au regard des contraintes d'exploitabilité et de droits d'accès

#### 3.4 Installation de l'infrastructure (C19)
- Rédiger la documentation de la procédure d'installation : prérequis, fichiers `docker-compose.yml`, variables d'environnement, ordre de démarrage, vérifications
- Tester la procédure d'installation dans un environnement de test (VM ou conteneurs) : elle doit se dérouler sans erreur
- Installer et vérifier le système de stockage : création des buckets ou espaces, accès à la console, test d'écriture et de lecture
- Installer et connecter au stockage les outils batch et temps réel, et vérifier qu'ils sont fonctionnels
- Installer le catalogue de données et le connecter au système de stockage
- Documenter l'installation et la configuration du stockage, des outils batch et de l'outil de catalogue

#### 3.5 Alimentation du data lake (C20)
- Présenter, pour chaque source de données à intégrer, la méthode d'alimentation retenue (batch, micro-batch ou temps réel) et la justifier
- Obtenir le flux temps réel : installer les dépendances du simulateur (`pip install numpy kafka-python`), puis le lancer depuis le dossier `generation/` (voir « Lancer le simulateur » dans `data/dictionnaire_donnees.md`). Il tourne en continu dans un terminal dédié et émet, à chaque minute simulée, un message par onduleur. Envoyez-le vers l'outil temps réel retenu (le simulateur propose une sortie Kafka, utilisable avec Redpanda ; avec un autre outil, adaptez la sortie ou écrivez votre propre producteur). Choisissez le nombre d'onduleurs (`--sites`, `--onduleurs-par-site`) selon les ressources de votre machine et justifiez ce choix
- Écrire les scripts d'alimentation :
  - batch : ingestion planifiée des fichiers (courbes de charge, contrats, météo, rapports d'intervention) vers la zone `raw`, avec partitionnement
  - temps réel : consommation du flux de télémétrie des onduleurs et écriture dans le stockage par lots réguliers
- Présenter le fonctionnement des programmes batch et temps réel (planification, reprise sur erreur, idempotence)
- Vérifier que les scripts s'exécutent sans erreur et que les données sont importées correctement (comptage, contrôle d'intégrité, échantillons)
- Appliquer, si nécessaire, une harmonisation des formats (ex : horodatages, unités, schémas des deux constructeurs d'onduleurs) pour produire la zone `staging`

#### 3.6 Gestion du catalogue de données (C20)
- Intégrer les métadonnées de chaque source dans le catalogue : description, propriétaire (« data owner »), source, fréquence de collecte, schéma, classification de sensibilité
- Documenter les colonnes clés (unités, plages de valeurs normales, signification des codes de défaut des onduleurs)
- Mettre à jour les métadonnées après chaque alimentation (automatiquement si possible) et le montrer lors d'une démonstration de recherche dans le catalogue

#### 3.7 Cycle de vie des données et procédures de suppression (C20)
- Définir le cycle de vie de chaque type de donnée (durée en zone active, archivage, suppression) en fonction des contraintes opérationnelles et réglementaires
- Programmer les procédures de mise à jour et de suppression (ex : règles de cycle de vie du stockage, tâche planifiée)
- Vérifier que les procédures de suppression respectent les contraintes d'accès, notamment réglementaires (droit à l'effacement), et les contraintes opérationnelles (ne pas supprimer une donnée encore utilisée)

#### 3.8 Monitorage et alertes (C20)
- Mettre en place le monitorage des conditions matérielles (espace disque disponible, mémoire, CPU) et applicatives (état des conteneurs et des services, retard de consommation du flux temps réel, échecs des traitements batch)
- Configurer une alerte (e-mail, notification…) lors d'une rupture de service
- Faire une démonstration : provoquer l'arrêt d'un service et montrer le déclenchement de l'alerte

#### 3.9 Conformité RGPD du data lake (C20)
- Rédiger le registre des traitements de données personnelles recensant chaque traitement du data lake (finalité, données concernées, base légale, durée de conservation, destinataires)
- Rédiger la procédure de tri des données personnelles (ex : agrégation ou suppression des courbes de charge fines au-delà de la durée de conservation, traitement des demandes d'effacement d'un foyer), en précisant le traitement appliqué, la fréquence d'exécution et son caractère automatisé ou manuel

#### 3.10 Gouvernance des accès (C21)
- Définir les groupes d'utilisateurs (ex : `data-engineer`, `data-analyst`, `data-scientist`, `admin`) et leurs besoins
- Configurer les droits d'alimentation du data lake (qui peut écrire dans quelle zone)
- Configurer les droits d'accès à l'index du catalogue pour la recherche de données
- Configurer les droits d'accès à la récupération des données (lecture par zone et par jeu de données)
- Appliquer les droits à des groupes et non à des individus dès que possible, et limiter chaque groupe aux seules ressources nécessaires
- Protéger les données sensibles : chiffrement au repos, restriction d'accès aux données personnelles, journaux d'audit activés et un exemple d'analyse
- Documenter les groupes d'accès, les droits associés, les droits individuels éventuels et la procédure de mise à jour des règles (arrivée, départ, changement de poste)
- Tester les droits (cas autorisés et cas refusés)



### 4. Données à intégrer et usages visés

#### Usages visés par le data lake

| N° | Usage futur                                                              | Données mobilisées                                         |
|----|--------------------------------------------------------------------------|------------------------------------------------------------|
| U1 | Détecter précocement les défaillances d'onduleurs (maintenance prédictive) | Télémétrie des onduleurs, rapports d'intervention         |
| U2 | Prévoir la production d'énergie de chaque centrale                       | Télémétrie des onduleurs, données météo                    |
| U3 | Suivre l'autoconsommation des foyers et produire les déclarations        | Courbes de charge, contrats                                |
| U4 | Analyser les rapports d'intervention et les photos thermographiques      | Rapports (PDF), images infrarouges                         |
| U5 | Retrouver rapidement n'importe quel jeu de données                       | Catalogue de données                                       |

#### Sources de données

| Source                           | Format                                          | Mode d'alimentation attendu  | Ordre de grandeur                                  |
|----------------------------------|-------------------------------------------------|------------------------------|----------------------------------------------------|
| Télémétrie des onduleurs         | JSON, flux continu (puissance, tension, température, code de défaut) | Temps réel | ≈ 12 millions de messages par jour (≈ 1,7 Go) |
| Courbes de charge des foyers (consommation et production) | CSV (données personnelles), pas de 30 minutes | Batch (quotidien)            | ≈ 600 000 lignes par jour                          |
| Contrats des foyers              | CSV (données personnelles)                      | Batch (quotidien)            | 12 000 lignes                                      |
| Relevés météo                    | CSV / JSON (rayonnement, nébulosité, température) | Micro-batch (horaire)      | 40 sites, ≈ 1 000 fichiers par jour                |
| Rapports d'intervention          | PDF + photos thermographiques (JPEG) + fichier JSON de métadonnées | Batch (à l'arrivée du fichier) | 8 000 fichiers (≈ 15 Go) |
| Référentiel des sites et des onduleurs | CSV                                       | Batch (ponctuel)             | 40 sites, ≈ 8 100 onduleurs                        |

Les volumes réels sont réduits dans l'environnement de test : le jeu batch fourni couvre 7 jours et 1 200 foyers, et le simulateur permet de régler le débit du flux temps réel (de quelques onduleurs à la flotte complète). Vous devrez néanmoins dimensionner l'architecture pour les volumes de production.

Les sources présentent volontairement des hétérogénéités : deux constructeurs d'onduleurs avec des schémas JSON différents, horodatages en UTC ou en heure locale, puissances en W ou en kW, messages en doublon, trous de transmission, codes de défaut non documentés. Elles font partie de l'exercice : vous devez les détecter, les traiter et les documenter.

Le dictionnaire de données vous est fourni avec les jeux de données. L'analyse de la section 3.1 doit le compléter et le vérifier.


# Modalités d'évaluation

Le rapport sera évalué sur :
- La cohérence des choix techniques avec le cadre d'exploitation et la justification de ces choix
- La qualité de l'architecture proposée et du schéma associé
- Le caractère fonctionnel et reproductible de l'installation et de l'alimentation
- La qualité de la gestion du catalogue, du cycle de vie des données et du monitorage
- La conformité au RGPD et la qualité de la gouvernance des accès
- La qualité de la documentation produite
- La soutenance orale de 10 minutes avec démonstration, suivie de questions du jury (5 minutes maximum)


# Livrables

Vous devrez remettre un rapport professionnel de 5 à 10 pages maximum couvrant l'ensemble des tâches réalisées. Ce rapport devra inclure :

- Une introduction présentant le contexte, les usages visés et le cadre technique
- L'analyse volume / vitesse / variété des sources et le schéma d'architecture, avec la justification des choix techniques
- La comparaison des catalogues de données et la justification du choix
- Pour chaque tâche : la méthodologie, les commandes clés, les captures d'écran ou extraits de code pertinents, et les résultats obtenus
- Les extraits essentiels de la procédure d'installation (prérequis, `docker-compose.yml`, vérifications)
- Les procédures de cycle de vie, de tri des données personnelles et le registre des traitements
- La matrice des droits d'accès et la documentation de gouvernance
- Une conclusion avec un retour d'expérience sur les outils utilisés

Seul le rapport est transmis au jury : il doit donc contenir tous les éléments utiles à l'évaluation (extraits de configuration et de scripts, captures d'écran, schémas, résultats). Le dépôt Git du projet (`docker-compose.yml`, scripts d'alimentation batch et temps réel, configuration du monitorage et des alertes, politiques d'accès, documentation) reste à votre usage, pour reproduire votre travail et préparer la démonstration.

Une soutenance orale individuelle de 10 minutes présente le rapport, avec une démonstration du catalogue connecté au stockage et du monitorage (déclenchement d'une alerte).


# Critères de performance

## C18 : Concevoir l'architecture du data lake en sélectionnant les technologies appropriées en fonction de la volumétrie, de la variété et de la vitesse des données dans le but de définir l'architecture technique optimale à intégrer

- Les propositions techniques sont cohérentes avec le cadre d'exploitation.
- Le schéma d'architecture tient compte des contraintes de volume, de vitesse et/ou de variété.
- Le schéma d'architecture est lisible et utilise un formalisme approprié.
- Plusieurs catalogues sont proposés et comparés au regard des contraintes.
- L'outil de catalogue sélectionné répond aux contraintes d'exploitabilité et de droits d'accès.


## C19 : Intégrer les différents composants d'infrastructure du data lake en appliquant la procédure adaptée, afin d'assurer l'acquisition, le stockage et la mise à disposition du catalogue de données

- L'ensemble des éléments de documentation nécessaires à la mise en œuvre de la procédure d'installation sont présentés.
- La procédure d'installation se déroule sans erreur dans un environnement de test.
- Le système de stockage est installé et fonctionnel en environnement de test.
- Les outils batch et temps réel sont fonctionnels et connectés au système de stockage.
- Le catalogue est connecté au système de stockage.
- La documentation couvre la procédure d'installation et de configuration du système de stockage, des outils batch et de l'outil de catalogue.


## C20 : Gérer le catalogue des données en tenant compte de leur nature, de leurs sources d'alimentation et de leur cycle de vie, dans le respect du RGPD, afin de garantir les fonctionnalités du service

- Les choix des méthodes d'alimentation sont justifiés et appropriés à chaque source de données.
- Les scripts d'alimentation s'exécutent sans erreur.
- Les données sont importées correctement dans le système de stockage.
- Les métadonnées sont intégrées dans le catalogue.
- Les procédures de suppression sont conformes aux contraintes d'accès (notamment réglementaires) et aux contraintes opérationnelles.
- Le monitorage permet le suivi des conditions matérielles et applicatives.
- Le monitorage génère une alerte lors d'une rupture de service.
- Le registre des traitements de données personnelles intègre l'ensemble des traitements de données personnelles impliqués dans le projet de data lake.
- Les procédures de tri des données personnelles pour la mise en conformité du data lake avec le RGPD sont rédigées.
- Les procédures de tri détaillent les traitements de conformité (automatisés ou non) à appliquer ainsi que leur fréquence d'exécution.


## C21 : Implémenter les règles de gouvernance des données en sécurisant la recherche, la récupération et l'ajout de données afin de respecter les règles de gouvernance des données de l'organisation, dans le cadre du déploiement de la politique de protection des données

- Les droits sont appliqués à des groupes et non à des individus dès que possible.
- Les accès fournis répondent aux besoins des groupes concernés.
- Les accès fournis sont limités aux ressources nécessaires aux usages des groupes concernés.
- Les accès fournis sont conformes à la réglementation (RGPD).
- La documentation couvre les groupes d'accès et les droits associés ainsi que les procédures de mise à jour des règles.
