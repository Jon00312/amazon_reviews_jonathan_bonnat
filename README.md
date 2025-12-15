# 🧠 Amazon Reviews ETL Pipeline

Ce projet implémente un pipeline **ETL complet (Extract → Transform → Load)** permettant de traiter des avis Amazon à partir d’une base transactionnelle PostgreSQL, de conserver une couche **Bronze rejouable**, de produire une couche **Silver propre et normalisée**, et de préparer les données pour des usages analytiques et NLP.

Le pipeline s’appuie sur :
- **PostgreSQL** comme source transactionnelle
- **MongoDB** comme couche **Bronze** (données brutes historisées)
- **Amazon S3** comme **Data Lake** (couche Silver en Parquet)
- une couverture de tests unitaires
- une orchestration Python modulaire (préparée pour Airflow)

---

# 1️⃣ Prérequis techniques

## Environnement requis
- Python ≥ 3.10  
- PostgreSQL (local ou Docker)  
- MongoDB (local ou Docker)  
- AWS S3 (bucket + credentials IAM)

## Installation
```bash
pip install -r requirements.txt
```

---

## 2️⃣ Configuration (.env)

La configuration se fait via le fichier `config/.env`.

### PostgreSQL
- **DATABASE_CREATION_URI**
- **DATABASE_SERVER_URI**
- **NEW_DATABASE_NAME**

### AWS S3
- **AWS_ACCESS_KEY_ID**
- **AWS_SECRET_ACCESS_KEY**
- **AWS_REGION**
- **S3_BUCKET**

### MongoDB
- **MONGO_URI**
- **MONGO_DB_NAME**

---

# 3️⃣ Architecture du pipeline

## 🟦 EXTRACT
- Connexion à PostgreSQL
- Extraction des tables :
  - buyer
  - subscription
  - product
  - orders
  - review
  - review_images
  - product_reviews
- Génération d’un audit d’extraction
- Gestion propre des erreurs

Sortie : dictionnaire de DataFrames

---

## 🟫 LOAD BRONZE → MongoDB

- Données stockées **brutes**
- Une collection MongoDB par table source (`bronze_*`)
- Permet de rejouer tout le pipeline sans PostgreSQL

---

## 🟪 TRANSFORM (Silver)

- Nettoyage du texte des avis
- Enrichissements :
  - product_id
  - has_image
  - has_subscription
  - verified_buyer
- Gestion explicite des rejets

Sorties :
- DataFrame Silver propre
- DataFrame des rejets

---

## 🟩 LOAD (Data Lake S3)

- Conversion en Parquet
- Upload vers :
  - cleaned/
  - rejects/
- Génération d’un audit de chargement

---

# 4️⃣ Tests unitaires 🧪

Outil : pytest

Couverture :
- extract : connexion DB, structure de sortie
- transform : nettoyage, enrichissements, rejets
- load : écriture parquet
- load_mongodb : chargement bronze

Lancer les tests :
```bash
python -m pytest
```

Résultat attendu :
```
= 16 passed in <5s =
```

---

# 5️⃣ Commandes principales

Lancement de la création BDD si besoin
```bash
python -m src.setup.create_database
```

Lancement global
```bash
python -m src.main
```

Lancement standalone
```bash
python -m src.etl.transform
python -m src.etl.extract
python -m src.etl.load_mongodb
python -m src.etl.transform
python -m src.etl.load
```

---

# 6️⃣ Dossiers générés

| Type | Destination |
|-----|------------|
| Bronze | MongoDB |
| Silver | S3 cleaned |
| Rejets | S3 rejects |
| Logs | logs/ |

---

# 7️⃣ Statut

✔ Pipeline fonctionnel  
✔ Tests validés  
✔ Architecture alignée avec le schéma présenté  
🚀 Prêt pour évolutions NLP et orchestration
