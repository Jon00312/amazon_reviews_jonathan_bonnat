import sys
import logging
from pathlib import Path
from datetime import datetime

# --- Import des modules internes ---
from src.setup.create_database import create_database  # si tu veux régénérer la base
from src.etl.extract import extract_all
from src.etl.transform import transform_all
from src.etl.load import load_all
from src.etl.load_mongodb import load_to_mongodb


# --- Initialisation du logging ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8",
    force=True
)
logger = logging.getLogger("main")

logger.info("=== [DÉMARRAGE DU PIPELINE ETL AMAZON REVIEWS] ===")

# --- Fonctions utilitaires ---
def safe_execute(step_name, func, *args, **kwargs):
    """Exécute une étape du pipeline en capturant les erreurs."""
    try:
        logger.info(f"🚀 Démarrage : {step_name}")
        result = func(*args, **kwargs)
        logger.info(f"✅ Terminé : {step_name}")
        return result
    except Exception as e:
        logger.error(f"❌ Erreur dans {step_name} : {e}", exc_info=True)
        print(f"⚠️ Erreur dans {step_name}. Voir logs/etl_pipeline.log pour le détail.")
        sys.exit(1)


# --- Pipeline complet ---
if __name__ == "__main__":
    start_time = datetime.now()
    print("=== 🚀 DÉMARRAGE DU PIPELINE ETL AMAZON REVIEWS ===\n")

    # 1️⃣ (Optionnel) Création / initialisation de la base
    #safe_execute("Création de la base PostgreSQL", create_database)

    # 2️⃣ Extraction des données
    data = safe_execute("EXTRACT", extract_all)

    # 3️⃣ Envoi vers MongoDB
    safe_execute("LOAD MONGODB", load_to_mongodb, data)

    # 4️⃣ Transformation
    df_reviews, df_rejects = safe_execute("TRANSFORM", transform_all)

    # 5️⃣ Chargement
    parquet_path = safe_execute("LOAD", load_all, df_reviews, df_rejects)

    # 6️⃣ Résumé final
    elapsed = (datetime.now() - start_time).total_seconds()
    summary = f"""
    🎯 PIPELINE TERMINÉ AVEC SUCCÈS
    - Temps total : {elapsed:.2f} sec
    - Fichier parquet généré : {parquet_path}
    - Logs : {LOG_DIR / 'etl_pipeline.log'}
    """
    print(summary)
    logger.info(summary)
    logger.info("=== [FIN DU PIPELINE ETL] ===\n")
