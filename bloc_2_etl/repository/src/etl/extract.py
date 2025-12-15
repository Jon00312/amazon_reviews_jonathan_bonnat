import os
import sys
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime


# ----------------------------------------------------
# ⚙️ CONFIGURATION GLOBALE
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
AUDIT_DIR = PROJECT_ROOT / "datalake" / "audit"

LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8"
)
logger = logging.getLogger("extract")
logger.info("\n=== [ÉTAPE : EXTRACT] ===\n")

# ----------------------------------------------------
# 🧱 FONCTION PRINCIPALE
# ----------------------------------------------------
def extract_all():
    """
    Extrait les tables nécessaires depuis PostgreSQL et renvoie un dictionnaire de DataFrames.
    Génère également un rapport d'audit dans datalake/audit/.
    """

    # --- Chargement du .env ---
    env_path = PROJECT_ROOT / "config" / ".env"
    if not env_path.exists():
        logger.error("Fichier de configuration .env introuvable.")
        sys.exit(1)

    load_dotenv(dotenv_path=env_path)
    DB_URI = os.getenv("DATABASE_SERVER_URI")

    if not DB_URI:
        logger.error("Variable DATABASE_SERVER_URI introuvable dans le .env.")
        sys.exit(1)

    logger.info("🚀 Démarrage de l'extraction.")
    logger.info(f"Connexion à la base : {DB_URI}")

    # --- Tables à extraire ---
    TABLES = [
        "buyer",
        "subscription",
        "product",
        "orders",
        "review",
        "review_images",
        "product_reviews"
    ]

    dataframes = {}
    summary = []

   # --- Connexion PostgreSQL ---
    try:
        engine = create_engine(DB_URI, pool_pre_ping=True)
        with engine.connect() as conn:
            logger.info("✅ Connexion PostgreSQL établie.")

            for table in TABLES:
                try:
                    df = pd.read_sql(f"SELECT * FROM {table};", conn)
                    dataframes[table] = df

                    rows = len(df)
                    nulls = df.isnull().sum().sum()
                    dups = df.duplicated().sum()

                    logger.info(
                        f"📦 {table}: {rows} lignes, {nulls} nulls, {dups} doublons."
                    )

                    summary.append({
                        "table": table,
                        "rows": rows,
                        "null_values": int(nulls),
                        "duplicates": int(dups)
                    })

                except Exception as e:
                    logger.warning(
                        f"⚠️ Erreur lors de l'extraction de '{table}': {e}"
                    )

    except OperationalError:
        logger.error("❌ Impossible de se connecter à PostgreSQL.")
        logger.error(
            "Vérifiez que le serveur est démarré et accessible "
            f"(DATABASE_SERVER_URI={DB_URI})"
        )
        sys.exit(1)

    except Exception:
        logger.exception("❌ Erreur inattendue lors de l'extraction.")
        sys.exit(1)

    logger.info("✅ Extraction terminée.")

    # --- Sauvegarde du rapport d'audit, en local pour debug dans S3 pour l'environnement de prod ---
    if summary:
        audit_df = pd.DataFrame(summary)
        audit_file = AUDIT_DIR / f"audit_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # --- Sauvegarde locale ---
        audit_df.to_csv(audit_file, index=False)
        logger.info(f"🧾 Rapport d'audit sauvegardé : {audit_file}")

    return dataframes


# ----------------------------------------------------
# 🧩 MODE STANDALONE
# ----------------------------------------------------
if __name__ == "__main__":
    data = extract_all()
    print("✅ Extraction terminée. Consultez logs/extract.log et datalake/audit/ pour les rapports.")
