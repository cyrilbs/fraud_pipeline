# Fraud Pipeline

[English](README.md)

Pipeline de détection de fraude en temps réel et analyses locales sur un lac Parquet alimenté en streaming.

Stack : Kafka, Spark Structured Streaming, scikit-learn, Cassandra, Parquet, DuckDB, dbt, Prometheus, Grafana

## Contexte

- Cas d'usage : surveiller des transactions potentiellement frauduleuses
- Besoin métier : détecter rapidement les transactions à risque
- Deux niveaux de réponse sont attendus :
  - décision au niveau transaction : ALLOW ou BLOCK
  - alerte agrégée : pic de fraude sur une micro-fenêtre
- Points clés : faible latence, traçabilité, analyse historique, supervision

## Architecture

![Schéma d'architecture](docs/images/architecture_overview.png)

- Étape 1 : générer une transaction JSON
- Étape 2 : publier la transaction dans `tpc_fraud`
- Étape 3 : scorer la transaction avec le modèle ML et publier dans `tpc_fraud_decisions`
- Étape 4 : traiter les événements avec Spark toutes les 5 secondes
- Étape 5 : stocker les décisions BLOCK dans Cassandra
- Étape 6 : stocker toutes les décisions dans Parquet
- Étape 7 : publier les alertes dans `tpc_alerts_aggregated`
- Étape 8 : Slack, dbt, Grafana

## Choix techniques : batch versus streaming

![Schéma batch versus streaming](docs/images/streaming.png)

Ce projet utilise le streaming parce que la détection de fraude est sensible au temps. Un pipeline batch serait utile pour du reporting historique, l'évaluation du modèle ou des agrégats quotidiens, mais il détecterait les transactions à risque trop tard pour permettre un blocage opérationnel. Dans ce schéma, tout est streaming sauf `dbt + DuckDB`.

Le streaming permet d'évaluer chaque transaction dès sa production, puis de la transmettre aux systèmes suivants avec une faible latence :

- Kafka découple les producteurs et les consommateurs.
- Le service ML score les transactions en continu.
- Spark Structured Streaming agrège les décisions sur de courtes fenêtres.
- Cassandra stocke les transactions bloquées pour une consultation rapide.
- Parquet conserve l'historique complet des décisions pour l'analyse et dbt.

Le batch reste utile dans cette architecture pour l'analyse offline, les tableaux de bord et l'amélioration du modèle. Le pipeline combine donc les deux approches : le streaming pour les décisions en temps réel, et des analyses de type batch sur Parquet pour l'historique.

## Choix techniques : Kafka

![Schéma Kafka](docs/images/kafka.png)

Kafka est utilisé comme colonne vertébrale événementielle du pipeline. Il découple la production des transactions, le scoring de fraude, l'agrégation streaming et les alertes, ce qui permet à chaque service d'évoluer et de redémarrer indépendamment.

Le pipeline utilise des topics séparés pour chaque étape :

- `tpc_fraud` reçoit les transactions brutes.
- `tpc_fraud_decisions` reçoit les décisions scorées par le service ML.
- `tpc_alerts_aggregated` reçoit les alertes de fraude agrégées par Spark.

Cette séparation garde les responsabilités claires et rend le flux de données plus simple à observer et à déboguer. Kafka fournit aussi un tampon : si un consommateur est temporairement indisponible, les événements restent disponibles dans le topic et peuvent être traités lorsque le service revient.

Kafka est donc adapté à ce projet parce que la détection de fraude nécessite une livraison d'événements à faible latence, de la rejouabilité pour le débogage et un couplage faible entre le producteur, le service de modèle, Spark et les alertes.

## Choix techniques : Spark

![Schéma Spark](docs/images/spark.png)

Spark Structured Streaming est utilisé pour traiter en continu les décisions de fraude scorées. Il lit les événements depuis Kafka, applique des transformations streaming et écrit les résultats vers du stockage opérationnel et analytique.

Dans ce pipeline, Spark a trois rôles principaux :

- Consommer les décisions scorées depuis `tpc_fraud_decisions`.
- Persister les transactions bloquées dans Cassandra pour une consultation opérationnelle.
- Écrire toutes les décisions dans Parquet pour l'analyse historique avec DuckDB et dbt.

Spark est utile ici parce qu'il fournit un modèle unifié pour le streaming et les traitements de type batch. Les mêmes concepts utilisés pour les transformations analytiques, comme les schémas, les agrégations et le fenêtrage, peuvent être appliqués à des flux d'événements en direct.

Le pipeline utilise des micro-batchs pour équilibrer latence et fiabilité. Cela rend le système suffisamment réactif pour la supervision de fraude, tout en gardant un traitement déterministe, traçable et plus simple à déboguer localement.

## Choix techniques : Parquet versus Avro

![Schéma Parquet](docs/images/parquet.png)

Parquet et Avro sont deux formats fréquents dans les pipelines de données, mais ils répondent à des usages différents.

Avro est orienté ligne et bien adapté au transport d'événements, à l'évolution de schéma et à la sérialisation de messages. C'est un bon choix lorsque des enregistrements sont échangés entre services, en particulier avec Kafka et un schema registry.

Parquet est orienté colonne et optimisé pour les charges analytiques. Il est plus efficace lorsque les requêtes lisent seulement un sous-ensemble de colonnes, agrègent de grands volumes ou scannent des données historiques.

Dans ce projet, Parquet est utilisé pour l'historique des décisions parce que les données sont ensuite requêtées par DuckDB, dbt et des outils analytiques. Les questions typiques portent sur des tendances, des comptages, des ratios et des fenêtres temporelles, ce qui bénéficie de l'organisation colonne et de la compression de Parquet.

Avro serait une option solide pour formaliser les contrats de messages Kafka dans une version production du pipeline. Pour ce projet local, JSON garde les messages Kafka faciles à inspecter, tandis que Parquet fournit un format efficace pour l'analyse.

## Service de scoring ML

![Schéma du service ML](docs/images/ml.png)

- Fichier : `model_service_kafka.py`
- Charge `models/fraud_model.pkl` avec joblib
- Variables utilisées :
  - `amount`
  - `country_risk`
- Modèle : `RandomForestClassifier`
- Seuil : `fraud_threshold_model = 0.8`
- Sortie : BLOCK si la probabilité est supérieure à 0.8, sinon ALLOW

## Choix techniques : Cassandra

![Schéma Cassandra](docs/images/cassandra.png)

Cassandra est utilisé pour stocker les transactions bloquées produites par le pipeline streaming. Ces enregistrements sont des données opérationnelles : ils représentent des transactions qui peuvent nécessiter une consultation rapide, une investigation ou une action en aval.

> **Note**
> - Données opérationnelles : utilisées maintenant par l'application ou les opérateurs pour agir.
> - Données analytiques : utilisées plus tard pour le reporting, l'exploration, les métriques et les tendances.

Cassandra convient à ce cas d'usage parce qu'il est conçu pour un fort débit d'écriture et des lectures à faible latence à grande échelle. Dans un contexte de détection de fraude, le système peut devoir ingérer beaucoup de décisions en continu tout en gardant les transactions bloquées récentes rapidement accessibles.

Dans ce projet, Spark écrit les décisions `BLOCK` dans Cassandra, tandis que toutes les décisions sont aussi stockées dans Parquet pour l'analyse. Cela sépare le stockage opérationnel du stockage analytique :

- Cassandra conserve le sous-ensemble actionnable des décisions.
- Parquet conserve le jeu de données historique complet.
- dbt et DuckDB interrogent Parquet au lieu de faire porter la charge analytique à Cassandra.

Cette conception garde Cassandra concentré sur les données opérationnelles de fraude, tandis que l'analyse historique est gérée par des formats et des outils plus adaptés aux scans et aux agrégations.

## Choix techniques : dbt

![Schéma dbt](docs/images/dbt.png)

dbt est utilisé pour structurer la couche analytique du projet. Il lit les fichiers Parquet écrits par le pipeline streaming et les transforme en modèles SQL réutilisables.

Dans ce projet, dbt ne fait pas partie du chemin de décision temps réel. Il s'exécute après que les données ont déjà été produites par Kafka, traitées par Spark et stockées dans Parquet. Son rôle est de rendre l'historique des décisions plus simple à requêter, documenter et étendre.

La couche dbt est utile parce qu'elle apporte des pratiques de génie logiciel à l'analytique :

- Les transformations SQL sont versionnées dans le dépôt.
- Les modèles sont organisés en staging et marts.
- Les définitions métier peuvent être centralisées et réutilisées.
- Des tests et de la documentation peuvent être ajoutés lorsque la couche analytique grandit.

DuckDB est utilisé comme moteur analytique local derrière dbt. Cela garde l'installation légère tout en permettant des requêtes efficaces sur des fichiers Parquet.

En résumé, dbt transforme la sortie brute du streaming en une couche analytique plus propre pour le reporting, l'exploration et de futurs tableaux de bord.

## Choix techniques : Prometheus et Grafana

![Schéma Prometheus et Grafana](docs/images/prom_grafana.png)

Prometheus et Grafana sont utilisés pour observer le comportement du pipeline de détection de fraude pendant son exécution.

Prometheus collecte les métriques exposées par les services Python, comme le nombre de messages produits, de décisions traitées, de messages en échec et la latence de traitement. Ces métriques permettent de détecter des problèmes opérationnels comme des consommateurs arrêtés, une hausse du taux d'erreur ou des délais de traitement anormaux.

Grafana fournit des tableaux de bord au-dessus de Prometheus. Il rend le pipeline plus simple à superviser visuellement en affichant les compteurs, les taux et les tendances de latence au même endroit.

Dans ce projet, l'observabilité est utile à la fois pour la supervision technique et métier :

- La supervision technique vérifie que les services fonctionnent correctement.
- La supervision métier vérifie si l'activité liée à la fraude change de façon inattendue.
- Les métriques de latence aident à vérifier que le pipeline reste proche du temps réel.
- Les métriques d'échec aident à identifier des intégrations cassées ou des messages mal formés.

Prometheus et Grafana sont donc bien adaptés à la supervision d'un pipeline de fraude streaming, où la fiabilité et la détection rapide sont importantes.

## Projet

```
fraud_pipeline/
├── docker-compose.yml       # Stack complète (infra + applications streaming)
├── README.md                # Documentation principale du projet
├── docs/images/archi.png    # Schémas d'architecture et de choix techniques
├── fraud_streaming/         # Kafka, Spark, modèle, alertes, lac Parquet
│   ├── src/app/             # Producteur, service modèle, job Spark, alertes, observabilité
│   ├── src/lib/             # Helpers partagés
│   ├── models/              # Artefact du modèle de fraude entraîné
│   ├── scripts/             # Script de création des topics Kafka
│   ├── tests/               # Tests unitaires
│   ├── docker-compose-kafka-cassandra.yml
│   ├── docker-compose-spark.yml
│   └── docker-compose.yml   # Services applicatifs uniquement (modulaire)
├── dbt/                     # DuckDB + dbt sur Parquet local
│   ├── models/staging/      # Modèles staging sur les données Parquet
│   ├── models/marts/        # Marts analytiques
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── docker-compose.yml
├── monitoring/              # Provisioning Prometheus et Grafana
│   ├── prometheus/
│   └── grafana/
├── docs/                    # Présentation et images générées
└── scripts/                 # Scripts utilitaires, dont la génération de présentation
```

## Démarrer toute la stack

### 1. Entraîner le modèle de fraude pour produire `fraud_model.pkl`

```bash
cd fraud_streaming
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/app/train_model.py
cd ..
```

### 2. Démarrer la stack complète

Depuis la racine du dépôt :

```bash
docker compose up -d
docker compose --profile demo up -d   # producteur de démo optionnel
```

- Spark UI : http://localhost:8080 (master), http://localhost:8081 (worker)
- Kafka : `localhost:9092`
- Cassandra : `localhost:9042`

> **Note sur l'image Docker**
>
> Les services Python (`model-service`, `alert-service`, `fraud-producer`) utilisent maintenant l'image construite depuis `fraud_streaming/src/app/Dockerfile` pour un démarrage plus rapide, avec les dépendances préinstallées.
>
> Pour reconstruire après une modification du code Python :
> ```bash
> docker compose build model-service
> ```

> **Note sur le lancement du producteur**
>
> Pour lancer uniquement le producteur sans Compose, Kafka et `model-service` doivent déjà être démarrés :
>
> ```bash
> docker run --rm -it \
>   --name kafka-fraud-producer \
>   --network data-platform-net \
>   -v "$PWD/fraud_streaming/src:/app" \
>   -w /app \
>   -e PYTHONPATH=/app \
>   -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
>   python:3.11 \
>   bash -c "pip install kafka-python && python app/fraud_producer.py"
> ```

### 3. Vérifications

Pour vérifier que `tpc_fraud` est correctement alimenté dans Kafka :

```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic tpc_fraud --from-beginning
```

Vérifications rapides Cassandra :

Utiliser `cqlsh` dans le conteneur Cassandra pour vérifier que le schéma est créé :

```bash
docker exec -it cassandra cqlsh

desc keyspaces;
USE mykeyspace;

desc tables;
desc table fraud;

select count(*) from fraud;
```

Pour vérifier Spark :

```bash
cd fraud_streaming
export PYTHONPATH=src
python src/app/validate_spark.py
```

### 4. Alertes Slack

- Créer d'abord une application Slack, par exemple `fraud_detection`, sur [https://api.slack.com/apps](https://api.slack.com/apps)
- Créer ensuite une URL Incoming Webhook pour votre canal
- Le client web Slack est disponible sur [https://app.slack.com/client](https://app.slack.com/client).

Définir `SLACK_WEBHOOK` dans le shell ou dans un fichier local `.env` avant de démarrer `alert-service`.

```bash
export SLACK_WEBHOOK="https://hooks.slack.com/services/..."
docker compose up -d --force-recreate alert-service
```

Pour désactiver Slack si nécessaire :

```bash
unset SLACK_WEBHOOK
docker compose up -d --force-recreate alert-service
```

### 5. Explorer Parquet avec dbt

Après avoir vérifié que le stream a écrit des données dans `fraud_streaming/data/parquet/`, on peut démarrer dbt :

```bash
cd dbt
docker compose run --rm dbt deps
docker compose run --rm dbt run
```

### 6. Observabilité

La stack Compose racine inclut Prometheus et Grafana. Prometheus collecte les métriques des services Python sur ces endpoints :

| Service | URL des métriques |
|---------|-------------------|
| `model-service` | http://localhost:9101/metrics |
| `alert-service` | http://localhost:9102/metrics |
| `fraud-producer` | http://localhost:9103/metrics |

Ouvrir Prometheus sur http://localhost:9090 pour interroger des métriques comme `fraud_processed_messages_total`, `fraud_produced_messages_total`, `fraud_failed_messages_total` et `fraud_message_processing_latency_seconds`.

Ouvrir Grafana sur http://localhost:3000 et se connecter avec `admin` / `admin`.

Le dashboard `Fraud Pipeline` est provisionné automatiquement dans le dossier `Fraud Pipeline`.

![Dashboard Grafana Fraud Pipeline](docs/images/grafana-fraud-pipeline-dashboard.png)

### 7. Arrêter les services

```bash
docker compose --profile demo down
docker compose down
```

## Un peu plus sur Spark

Les applications Spark sont coordonnées par un driver et exécutées par des workers via des executors.

Le driver est le processus principal d'une application Spark. Il construit le plan d'exécution, coordonne le job et envoie les tâches aux executors. Dans ce projet, le job streaming défini dans `fraud_streaming.py` joue le rôle d'application Spark.

Le master appartient au gestionnaire de cluster Spark. Il suit les workers disponibles et attribue les ressources aux applications. Dans la configuration Docker locale, le Spark master fournit le point d'entrée du cluster utilisé par le job streaming.

Les workers sont les machines ou conteneurs qui fournissent CPU et mémoire au cluster. Ils n'exécutent pas directement toute la logique applicative ; ils hébergent les executors.

Les executors sont des processus lancés sur les workers pour une application Spark donnée. Ils exécutent les tâches envoyées par le driver, gardent des données intermédiaires en mémoire si nécessaire et écrivent les résultats vers des systèmes externes comme Cassandra ou Parquet.

En résumé :

- Le driver planifie et coordonne l'application Spark.
- Le master gère les ressources du cluster.
- Les workers fournissent la capacité de calcul.
- Les executors exécutent les tâches Spark.

Dans ce projet, ce modèle permet au job de fraude streaming de consommer les événements Kafka, de les traiter en micro-batchs et d'écrire les résultats en continu, tout en gardant une exécution distribuée et observable via la Spark UI.

## Limites du projet

Ce projet est conçu comme un pipeline local de détection de fraude de bout en bout, donc certains aspects de production sont simplifiés.

Les messages Kafka utilisent JSON, ce qui est facile à inspecter et à déboguer, mais n'impose pas de schémas forts comme Avro ou Protobuf avec un schema registry. Le modèle ML est aussi volontairement simple et utilise un nombre limité de variables ; il doit donc être vu comme un modèle de démonstration plutôt qu'un modèle de fraude de production.

L'infrastructure fonctionne localement avec Docker Compose. Cela rend le projet facile à reproduire, mais n'inclut pas des fonctionnalités de déploiement production comme l'autoscaling, la haute disponibilité, la gestion des secrets, le contrôle d'accès ou la reprise après sinistre.

L'observabilité est présente avec Prometheus et Grafana, mais les règles d'alerte et les workflows d'incident restent limités.

## Améliorations

Une version production pourrait introduire des contrats de messages plus stricts avec Avro ou Protobuf et un schema registry. Cela rendrait les événements Kafka plus sûrs à faire évoluer et plus simples à valider entre services.

Le modèle de fraude pourrait aussi être amélioré avec des variables plus réalistes, de meilleures données d'entraînement, du versioning de modèle et une surveillance du drift. Un registre de modèles dédié pourrait aider à suivre les modèles déployés et à revenir en arrière si nécessaire.

L'infrastructure pourrait être déployée sur Kubernetes ou une plateforme managée, avec une gestion correcte des secrets, des limites de ressources, des politiques de scaling et du stockage persistant. Des tests supplémentaires pourraient couvrir des scénarios d'intégration entre Kafka, Spark, Cassandra et le service de modèle.

L'observabilité pourrait être étendue avec des règles d'alerte, des indicateurs de niveau de service et des dashboards centrés à la fois sur la santé technique et les métriques métier liées à la fraude.

## Conclusion

Ce projet montre comment un pipeline de détection de fraude en temps réel peut combiner streaming d'événements, machine learning, stockage opérationnel, stockage analytique et observabilité.

Kafka fournit la colonne vertébrale événementielle, le service ML score les transactions, Spark traite les décisions en continu, Cassandra stocke les transactions bloquées actionnables, et Parquet conserve l'historique complet disponible pour l'analyse avec DuckDB et dbt.

Le résultat est une architecture compacte mais complète qui illustre les principaux blocs d'une plateforme de données streaming moderne.

## Dépannage

### Spark : `UnknownTopicOrPartitionException` sur `tpc_fraud_decisions`

Spark démarre avant que le topic Kafka existe. La stack lance maintenant `kafka-init` pour créer `tpc_fraud`, `tpc_fraud_decisions` et `tpc_alerts_aggregated` avant `spark-driver`.

Si Kafka a été réinitialisé ou si l'erreur apparaît encore, nettoyer le checkpoint streaming et recréer le driver :

```bash
docker compose stop spark-driver
rm -rf fraud_streaming/.checkpoint
docker compose up -d kafka-init
docker compose up -d spark-driver
```

Vérifier que le producteur de démo ou le service modèle envoie bien des événements afin que `tpc_fraud_decisions` reçoive des données.

### Spark : l'offset Kafka a changé ou des données peuvent avoir été perdues

Cela arrive lorsque le checkpoint Spark contient encore d'anciens offsets Kafka alors que le topic Kafka a été réinitialisé, supprimé ou recréé. Le job Spark local utilise par défaut `SPARK_FAIL_ON_DATA_LOSS=false` afin que le stream puisse récupérer pendant le développement, mais nettoyer le checkpoint obsolète reste la réinitialisation la plus propre :

```bash
docker compose stop spark-driver
rm -rf fraud_streaming/.checkpoint/fraud_decisions
docker compose up -d spark-driver
```

Pour un comportement plus strict et plus proche de la production, définir `SPARK_FAIL_ON_DATA_LOSS=true` pour `spark-driver`.

### Spark : `Mkdirs failed` lors de l'écriture Parquet

Les fichiers Parquet sont écrits par les **executors** Spark sur `spark-worker`, pas seulement par `spark-driver`. Le worker doit monter `fraud_streaming` dans `/streaming`, comme configuré dans `docker-compose.yml`. Après un changement de montage, recréer le worker et le driver :

```bash
docker compose up -d --force-recreate spark-worker spark-driver
```
