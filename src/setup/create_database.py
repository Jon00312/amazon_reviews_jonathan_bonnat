"""
Script de création de la BDD PostgreSQL
en récupérant les fichiers raw sur S3

Ne fait pas partie intégrante du pipeline ETL
mais peut être exécutée lors de l'installation du projet
en local ou pour les tests

Si la base existe déjà, le script la supprime pour la recréer
à partir des sources
"""

import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
from botocore.exceptions import NoCredentialsError, ClientError
import boto3

def create_database():
    # ----------------------------------------------------
    # ⚙️ CONFIGURATION & LOGGING
    # ----------------------------------------------------
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    LOG_DIR = PROJECT_ROOT / "logs"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        filename=LOG_DIR / "create_db.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        encoding="utf-8",
        force=True
    )
    logger = logging.getLogger("create_db")
    logger.info("________________________________________________")

    # Chargement du fichier .env
    env_path = PROJECT_ROOT / "config" / ".env"
    if not env_path.exists():
        logger.error("Fichier de configuration .env introuvable.")
        sys.exit(1)

    load_dotenv(dotenv_path=env_path)
    
    # Lecture des variables d'environnement
    ROOT_URI = os.getenv("DATABASE_CREATION_URI")
    DB_URI = os.getenv("DATABASE_SERVER_URI")
    DB_NAME = os.getenv("NEW_DATABASE_NAME")

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_REGION")
    S3_BUCKET = os.getenv("S3_BUCKET")

    TABLES = ["buyer", "subscription", "product", "orders", "review", "review_images", "product_reviews"]
    

    # ----------------------------------------------------
    # 🧱 FONCTION PRINCIPALE : CRÉATION + ALIMENTATION
    # ----------------------------------------------------
    logger.info("🚀 Démarrage de la création de la base PostgreSQL depuis S3.")

    # --- Connexion à la base racine ---
    try:
        root_engine = create_engine(ROOT_URI)
        if not DB_NAME:
            logger.error("NEW_DATABASE_NAME non défini.")
            return False
        with root_engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME};"))
            conn.execute(text(f"CREATE DATABASE {DB_NAME};"))
            logger.info(f"✅ Base '{DB_NAME}' créée ou réinitialisée avec succès.")
    except Exception as e:
        logger.exception("Erreur lors de la création de la base.")
        return False

    # --- Connexion à la nouvelle base ---
    try:
        db_engine = create_engine(DB_URI)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        logger.info(f"Connexion établie avec la base '{DB_NAME}'.")
    except Exception as e:
        logger.exception(f"Échec de la connexion à la base '{DB_NAME}'.")
        return False



    # --- Importation des tables via URL S3 ---
    logger.info(f"📦 Lecture des fichiers depuis s3://{S3_BUCKET}")
    for table in TABLES:
        s3_key = f"raw/{table}.csv"
        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

        try:
            logger.info(f"📄 Chargement de {s3_uri}")
            df = pd.read_csv(
                s3_uri,
                storage_options={
                    "key": AWS_ACCESS_KEY_ID,
                    "secret": AWS_SECRET_ACCESS_KEY
                }
            )
            # Chargement des données brutes sans transformation
            df.to_sql(table, db_engine, if_exists="replace", index=False)
            count = pd.read_sql(f"SELECT COUNT(*) FROM {table}", db_engine).iloc[0, 0]
            logger.info(f"✅ Table '{table}' importée ({count} lignes).")

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(f"⚠️ Fichier introuvable sur S3 : {s3_key}")
            else:
                logger.error(f"❌ Erreur lors du chargement de {s3_key} : {e}")
        except Exception as e:
            logger.exception(f"Erreur inattendue lors de l’import de {s3_key} : {e}")

    logger.info("🎉 Base PostgreSQL initialisée avec succès depuis les fichiers S3.")
    logger.info("________________________________________________")
    print("✅ Création et import terminés depuis S3. Consultez 'logs/create_db.log' pour le détail.")
    return True


# ----------------------------------------------------
# 🧩 MODE STANDALONE
# ----------------------------------------------------
if __name__ == "__main__":
    success = create_database()
    if success:
        print("✅ Base créée et alimentée depuis S3.")
    else:
        print("❌ Échec de la création de la base. Voir les logs pour plus de détails.")
