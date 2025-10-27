# Création du schéma et des tables 

import sys
import logging
import psycopg2
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, LOGGING

logging.basicConfig(
    level=getattr(logging, LOGGING.get("level", "INFO")),
    format=LOGGING.get("format", "%(message)s"),
    handlers=[
        logging.FileHandler(LOGGING.get("file", "etl_pipeline.log")),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

def activ_postgis(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()
    except Exception as e:
        try:
            logger.warning(f"Impossible d'activer PostGIS automatiquement : {e}")
        except Exception:
            pass

# Nom du schéma 
NOM_SCHEMA = "vc_etl"

DDL_SQL = {
    "schema": f"""
        CREATE SCHEMA IF NOT EXISTS {NOM_SCHEMA};
    """,
    "val_cenis": f"""
        CREATE TABLE IF NOT EXISTS {NOM_SCHEMA}.val_cenis (
            fid BIGSERIAL PRIMARY KEY,
            gml_id TEXT,
            cleabs TEXT,
            nom TEXT,
            statut TEXT,
            population INTEGER,
            insee_code TEXT,
            date_recensement TIMESTAMP,
            insee_canton TEXT,
            insee_arr TEXT,
            insee_dep TEXT,
            siren_code TEXT,
            postal_code TEXT,
            supf_cadas INTEGER
        );
    """,
    "ban": f"""
        CREATE TABLE IF NOT EXISTS {NOM_SCHEMA}.ban (
            fid BIGSERIAL PRIMARY KEY
            -- autres colonnes ajoutées dynamiquement au chargement
        );
    """,
    "sommets": f"""
        CREATE TABLE IF NOT EXISTS {NOM_SCHEMA}.sommets (
            fid BIGSERIAL PRIMARY KEY,
            osm_id TEXT,
            nom TEXT,
            altitude DOUBLE PRECISION
        );
    """,
    "batiments": f"""
        CREATE TABLE IF NOT EXISTS {NOM_SCHEMA}.batiments (
            fid BIGSERIAL PRIMARY KEY,
            cleabs TEXT,
            nature TEXT,
            usage1 TEXT,
            usage2 TEXT,
            construc_legere BOOLEAN,
            etat_obj TEXT,
            date_crea TIMESTAMP,
            date_modif TIMESTAMP,
            date_apparition DATE,
            date_confirm DATE,
            sources TEXT,
            id_sources TEXT,
            methode_acquis_plani TEXT,
            methode_acquis_alti TEXT,
            precision_plani REAL,
            precision_alti REAL,
            nombre_logements INTEGER,
            nombre_etages INTEGER,
            materiaux_murs TEXT,
            materiaux_toiture TEXT,
            hauteur DOUBLE PRECISION,
            alti_mini_sol DOUBLE PRECISION,
            alti_mini_toit DOUBLE PRECISION,
            alti_max_toit DOUBLE PRECISION,
            alti_max_sol DOUBLE PRECISION,
            origine_bat TEXT,
            appariement_fonciers TEXT,
            id_rnb TEXT
        );
    """
}

def creer_schema_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_SQL["schema"])
        logger.info(f"Schéma {NOM_SCHEMA} prêt.")
        for nom_table in ("val_cenis", "ban", "sommets", "batiments"):
            cur.execute(DDL_SQL[nom_table])
            logger.info(f"Table {NOM_SCHEMA}.{nom_table} prête.")

def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        conn.autocommit = False
        activ_postgis(conn)  # Activation PostGIS si pas crée au moment de la création de la bdd "etl_vc"
        creer_schema_tables(conn)
        conn.commit()
        logger.info("Création terminée.")
        return 0
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.error(f"Erreur : {e}")
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(main())
