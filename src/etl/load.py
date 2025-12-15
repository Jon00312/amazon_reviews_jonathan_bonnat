import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from src.etl.transform import transform_all
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# ----------------------------------------------------
# ⚙️ CONFIGURATION & LOGGING
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
CLEANED_DIR = PROJECT_ROOT / "datalake" / "cleaned"
PROCESSED_DIR = PROJECT_ROOT / "datalake" / "processed"
REJECTS_DIR = PROJECT_ROOT / "datalake" / "rejects"

LOG_DIR.mkdir(parents=True, exist_ok=True)
CLEANED_DIR.mkdir(parents=True, exist_ok=True)
REJECTS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8",
    force=True
)
logger = logging.getLogger("load")
logger.info("=== [ÉTAPE : LOAD] ===")

# Chargement du .env
env_path = PROJECT_ROOT / "config" / ".env"
if not env_path.exists():
    logger.error("Fichier .env introuvable.")
    raise SystemExit("Erreur : fichier .env manquant.")
load_dotenv(dotenv_path=env_path)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")

# ----------------------------------------------------
# 🧱 FONCTION PRINCIPALE
# ----------------------------------------------------
def load_all(df_reviews=None, df_rejects=None):
    """
    Étape de LOAD :
    - Charge les données transformées (CSV)
    - Les convertit en Parquet pour la couche silver du data lake
    - Envoie le fichier vers S3
    - Produit un audit de chargement enrichi
    """

    logger.info("🚀 Démarrage de l'étape de LOAD.")

    # --- Étape 1 : Récupération des données transformées ---
    if df_reviews is None:
        # Cherche le dernier CSV local généré par transform_all() ou le lance
        reviews_csv_files = sorted(PROCESSED_DIR.glob("reviews_cleaned_*.csv"), key=os.path.getmtime)
        if not reviews_csv_files:
            logger.warning("Aucun fichier transformé trouvé. Exécution du transform_all()...")
            df_reviews, df_rejects = transform_all()
        else:
            latest_csv = reviews_csv_files[-1]
            logger.info(f"📥 Chargement du fichier transformé : {latest_csv.name}")
            df_reviews = pd.read_csv(latest_csv)

    if df_reviews.empty:
        logger.error("Le DataFrame fourni est vide. Abandon du chargement.")
        raise SystemExit("Erreur : aucune donnée à charger.")
    

    # On récupère aussi les rejets
    if df_rejects is None:
        reject_files = sorted(REJECTS_DIR.glob("reviews_rejects_*.csv"), key=os.path.getmtime)
        if reject_files:
            latest_reject = reject_files[-1]
            logger.info(f"📥 Chargement des rejets depuis : {latest_reject.name}")
            df_rejects = pd.read_csv(latest_reject)
        else:
            df_rejects = pd.DataFrame()


    # --- Étape 2 : Conversion en Parquet ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    parquet_file = CLEANED_DIR / f"reviews_cleaned_{timestamp}.parquet"

    try:
        df_reviews.to_parquet(parquet_file, index=False)
        logger.info(f"✅ Données chargées dans le data lake : {parquet_file}")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'écriture du fichier Parquet : {e}")
        raise

    # --- Étape 3 : Upload vers S3 ---
    s3_uri = None
    try:
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET, AWS_REGION]):
            raise EnvironmentError("Variables d'environnement AWS manquantes.")

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        s3_key = f"cleaned/{parquet_file.name}"
        s3_client.upload_file(str(parquet_file), S3_BUCKET, s3_key)
        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

        logger.info(f"📤 Fichier uploadé sur S3 : {s3_uri}")
        print(f"📤 Fichier uploadé sur S3 : {s3_uri}")

    except (NoCredentialsError, ClientError, EnvironmentError) as e:
        logger.error(f"❌ Erreur lors de l'upload vers S3 : {e}")
        s3_uri = None

    # --- Étape 3 bis : Upload vers s3 des rejets (si présents) ---
    if df_rejects is not None and not df_rejects.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reject_key = f"rejects/reviews_rejects_{timestamp}.csv"

        try:
            df_rejects.to_csv(
                f"s3://{S3_BUCKET}/{reject_key}",
                index=False,
                storage_options={
                    "key": AWS_ACCESS_KEY_ID,
                    "secret": AWS_SECRET_ACCESS_KEY
                }
            )
            logger.warning(f"⚠️ {len(df_rejects)} lignes rejetées envoyées sur s3://{S3_BUCKET}/{reject_key}")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'envoi des rejets sur S3 : {e}")


    # --- Étape 4 : Audit du chargement ---
    audit = {
        "timestamp": datetime.now().isoformat(),
        "rows_loaded": len(df_reviews),
        "rows_rejected": len(df_rejects) if df_rejects is not None else 0,
        "columns": list(df_reviews.columns),
        "file_name": parquet_file.name,
        "file_size_MB": round(os.path.getsize(parquet_file) / (1024 * 1024), 3),
        "avg_rating": df_reviews["rating"].mean() if "rating" in df_reviews.columns else None,
        "nb_with_images": int(df_reviews["has_image"].sum()) if "has_image" in df_reviews.columns else None,
        "nb_verified_buyers": int(df_reviews["verified_buyer"].sum()) if "verified_buyer" in df_reviews.columns else None,
        "s3_uri": s3_uri
    }

    audit_file = CLEANED_DIR / f"audit_load_{timestamp}.csv"
    pd.DataFrame([audit]).to_csv(audit_file, index=False)
    logger.info(f"📊 Rapport d'audit enregistré : {audit_file}")

    logger.info("🎉 Étape LOAD terminée avec succès.")
    return parquet_file


# ----------------------------------------------------
# 🧩 MODE STANDALONE
# ----------------------------------------------------
if __name__ == "__main__":
    parquet_path = load_all()
    print(f"✅ Données chargées dans le data lake : {parquet_path}")
