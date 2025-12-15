import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient

# ----------------------------------------------------
# ⚙️ CONFIGURATION & LOGGING
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8",
    force=True
)
logger = logging.getLogger("load_mongodb")
logger.info("=== [ÉTAPE : LOAD BRONZE → MONGODB] ===")

# ----------------------------------------------------
# 🔐 Chargement de la configuration
# ----------------------------------------------------
env_path = PROJECT_ROOT / "config" / ".env"
if not env_path.exists():
    logger.error("❌ Fichier .env introuvable.")
    sys.exit(1)

load_dotenv(env_path)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

if not all([MONGO_URI, MONGO_DB_NAME]):
    logger.error("❌ Configuration MongoDB incomplète.")
    sys.exit(1)

# ----------------------------------------------------
# 🧩 Connexion MongoDB
# ----------------------------------------------------
try:
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]
    logger.info("✅ Connexion MongoDB établie.")
except Exception as e:
    logger.error(f"❌ Erreur de connexion MongoDB : {e}")
    sys.exit(1)

# ----------------------------------------------------
# 🧱 Chargement Bronze MongoDB
# ----------------------------------------------------
def build_document(table_name: str, record: dict, run_id: str) -> dict:
    return {
        "run_id": run_id,
        "source_table": table_name,
        "extracted_at": datetime.now(timezone.utc),
        "data": record
    }

def load_to_mongodb(data: dict | None = None):
    """
    Charge les données brutes issues de extract_all() dans MongoDB (couche Bronze).

    - Une collection MongoDB par table source
    - Données stockées sans transformation métier
    - Permet de rejouer tout le pipeline indépendamment de PostgreSQL
    """

    logger.info("🚀 Démarrage du chargement Bronze MongoDB.")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Run ID : {run_id}")

    # --- Étape 1 : récupération des données ---
    if data is None:
        logger.info("Aucune donnée fournie → lancement de extract_all().")
        from src.etl.extract import extract_all #import local pour éviter la dépendance inutile
        data = extract_all()

    if not isinstance(data, dict) or not data:
        logger.error("❌ Données d'extraction invalides ou vides.")
        sys.exit(1)

    # --- Étape 2 : chargement table par table ---
    for table_name, df in data.items():

        if df.empty:
            logger.warning(f"⚠️ Table '{table_name}' vide — ignorée.")
            continue

        collection_name = f"bronze_{table_name}"
        collection = mongo_db[collection_name]

        records = df.to_dict(orient="records")

        documents = [
            build_document(table_name, record, run_id)
            for record in records
        ]

        try:
            collection.delete_many({"run_id": run_id})
            result = collection.insert_many(documents)
            logger.info(
                f"✅ {len(result.inserted_ids)} documents insérés dans '{collection_name}'."
            )
        except Exception as e:
            logger.error(
                f"❌ Erreur d'insertion MongoDB pour '{collection_name}' : {e}"
            )
            raise

    logger.info("🎉 Chargement Bronze MongoDB terminé avec succès.")

# ----------------------------------------------------
# 🧩 MODE STANDALONE
# ----------------------------------------------------
if __name__ == "__main__":
    load_to_mongodb()
    print("✅ Données Bronze chargées dans MongoDB.")
