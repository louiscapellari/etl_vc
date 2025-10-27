from pathlib import Path
import os

# Dossiers du projet
DOSSIER_BASE = Path(__file__).parent.resolve()     # racine 
DOSSIER_DONNEES = DOSSIER_BASE / "data"            # données brutes récupérées

if os.name == "nt":
    docs = Path(os.environ.get("USERPROFILE", str(Path.home()))) / "Documents"
    DOSSIER_TEMP = Path(os.getenv("ETL_TEMP_DIR", str(docs / "vc_tmp")))
else:
    DOSSIER_TEMP = Path(os.getenv("ETL_TEMP_DIR", str((DOSSIER_BASE / "temp").resolve())))

DOSSIER_SORTIE = DOSSIER_BASE / "output"           # exports finaux
DOSSIER_LOGS = DOSSIER_BASE / "logs"               # fichiers de logs

for d in (DOSSIER_DONNEES, DOSSIER_TEMP, DOSSIER_SORTIE, DOSSIER_LOGS):
    d.mkdir(parents=True, exist_ok=True)

# Systèmes de coordonnées
CRS_ENTREE = "EPSG:4326"
CRS_CIBLE  = "EPSG:2154"


# Constantes services
URL_WFS_BASE = "https://data.geopf.fr/wfs/ows"
NOM_COMMUNE = "Val-Cenis"

WFS_BASE_URL = URL_WFS_BASE
COMMUNE_NAME = NOM_COMMUNE

def like(field: str, value: str) -> str:
    # écriture du filtre du WFS
    return f'"{field}" LIKE \'{value}\''

# Connexion bdd
BASE_DE_DONNEES = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "database": os.getenv("PGDATABASE", "etl_vc"),
    "user": os.getenv("PGUSER", "postgres"),        # Renseignez le nom de d'utilisateur si différent
    "password": os.getenv("PGPASSWORD", ""),   # Mot de passe à renseigner
}

DATABASE = BASE_DE_DONNEES

# Paramètres couche commune de Val-Cenis
CONF_VAL_CENIS = {
    "table_name": "val_cenis",
    "wfs_url": URL_WFS_BASE,
    "layer_name": "LIMITES_ADMINISTRATIVES_EXPRESS.LATEST:commune",
    "filter": like("nom_officiel", NOM_COMMUNE),
    # transformation des champs
    "fields_mapping": {
        "fid": None,
        "gml_id": "gml_id",
        "cleabs": "cleabs",
        "nom": "nom_officiel",
        "statut": "statut",
        "population": "population",
        "insee_code": "code_insee",
        "date_recensement": "date_recensement",
        "insee_canton": "code_insee_du_canton",
        "insee_arr": "code_insee_de_l_arrondissement",
        "insee_dep": "code_insee_du_departement",
        "siren_code": "code_siren",
        "postal_code": "code_postal",
        "supf_cadas": "superficie_castrale",
    },
    # types
    "field_types": {
        "fid": "INTEGER PRIMARY KEY",
        "gml_id": "TEXT",
        "cleabs": "TEXT",
        "nom": "TEXT",
        "statut": "TEXT",
        "population": "INTEGER",
        "insee_code": "TEXT",
        "date_recensement": "TIMESTAMP",
        "insee_canton": "TEXT",
        "insee_arr": "TEXT",
        "insee_dep": "TEXT",
        "siren_code": "TEXT",
        "postal_code": "TEXT",
        "supf_cadas": "INTEGER",
    },
}

VAL_CENIS = CONF_VAL_CENIS

# Paramètres BAN
CONF_BAN = {
    "table_name": "ban",
    "wfs_url": URL_WFS_BASE,
    "layer_name": "BAN.DATA.GOUV:ban",
    "filter": like("nom_commune", NOM_COMMUNE),
    "clip_by_valcenis": True,
    "add_fid": True,
}

BAN = CONF_BAN

# Paramètres Sommets OSM
CONF_SOMMETS = {
    "table_name": "sommets",
    "source_type": "osm_download",
    "download_url": "https://download.geofabrik.de/europe/france/rhone-alpes-latest-free.shp.zip",
    "layer_name": "gis_osm_natural_free_1",
    "filter": "fclass = 'peak'",
    "clip_by_valcenis": True,
    "fields_mapping": {
        "fid": None,
        "osm_id": "osm_id",
        "nom": "name",
        "elevation": "ele",
    },
    "field_types": {
        "fid": "INTEGER PRIMARY KEY",
        "osm_id": "TEXT",
        "nom": "TEXT",
        "elevation": "INTEGER",
    },
}

SOMMETS = CONF_SOMMETS

# Paramètres Bâtiments (BD TOPO)
CONF_BATIMENTS = {
    "table_name": "batiments",
    "source_type": "bdtopo_download",
    "catalog_page": "https://geoservices.ign.fr/bdtopo",
    "dept_code": "D073",
    "dept_label": "Savoie",
    "format_keyword": "GPKG",
    "crs": "LAMB93",
    "expected_layer_contains": "batiment",
    "download_subdir": "bd",   
    "keep_archive": True,
    "fields_mapping": {
        "fid": None,
        "cleabs": "cleabs",
        "nature": "nature",
        "usage1": "usage_1",
        "usage2": "usage_2",
        "construc_legere": "construction_legere",
        "etat_obj": "etat_de_l_objet",
        "date_crea": "date_creation",
        "date_modif": "date_modification",
        "date_apparition": "date_d_apparition",
        "date_confirm": "date_de_confirmation",
        "sources": "sources",
        "id_sources": "identifiants_sources",
        "methode_acquis_plani": "methode_d_acquisition_planimetrique",
        "methode_acquis_alti": "methode_d_acquisition_altimetrique",
        "precision_plani": "precision_planimetrique",
        "precision_alti": "precision_altimetrique",
        "nombre_logements": "nombre_de_logements",
        "nombre_etages": "nombre_d_etages",
        "materiaux_murs": "materiaux_des_murs",
        "materiaux_toiture": "materiaux_de_la_toiture",
        "hauteur": "hauteur",
        "alti_mini_sol": "altitude_minimale_sol",
        "alti_mini_toit": "altitude_minimale_toit",
        "alti_max_toit": "altitude_maximale_toit",
        "alti_max_sol": "altitude_maximale_sol",
        "origine_bat": "origine_du_batiment",
        "appariement_fonciers": "appariement_fichiers_fonciers",
        "id_rnb": "identifiants_rnb",
    },
    "field_types": {  # types
        "fid": "INTEGER PRIMARY KEY",
        "cleabs": "TEXT",
        "nature": "TEXT",
        "usage1": "TEXT",
        "usage2": "TEXT",
        "construc_legere": "BOOLEAN",
        "etat_obj": "TEXT",
        "date_crea": "TIMESTAMP",
        "date_modif": "TIMESTAMP",
        "date_apparition": "DATE",
        "date_confirm": "DATE",
        "sources": "TEXT",
        "id_sources": "TEXT",
        "methode_acquis_plani": "TEXT",
        "methode_acquis_alti": "TEXT",
        "precision_plani": "REAL",
        "precision_alti": "REAL",
        "nombre_logements": "INTEGER",
        "nombre_etages": "INTEGER",
        "materiaux_murs": "TEXT",
        "materiaux_toiture": "TEXT",
        "hauteur": "DOUBLE PRECISION",
        "alti_mini_sol": "DOUBLE PRECISION",
        "alti_mini_toit": "DOUBLE PRECISION",
        "alti_max_toit": "DOUBLE PRECISION",
        "alti_max_sol": "DOUBLE PRECISION",
        "origine_bat": "TEXT",
        "appariement_fonciers": "TEXT",
        "id_rnb": "TEXT",
    },
}

BATIMENTS = CONF_BATIMENTS

LOGGING = {
    "level": "INFO",
    "format": "%(message)s",
    "file": str((DOSSIER_LOGS / "etl_pipeline.log").resolve()),
}

# maj (pipeline.py)
CONFIG_MAJ = {
    "frequency": "monthly",
    "backup_before_update": True,
    "keep_backup_days": 90,
}

UPDATE_CONFIG = CONFIG_MAJ


def recup_bdd_url() -> str:
    return (
        f"postgresql://{BASE_DE_DONNEES['user']}:{BASE_DE_DONNEES['password']}"
        f"@{BASE_DE_DONNEES['host']}:{BASE_DE_DONNEES['port']}/{BASE_DE_DONNEES['database']}"
    )

def recup_chemin_export(table_name: str) -> Path:
    # chemin d’export GPKG pour une table
    return DOSSIER_SORTIE / f"{table_name}.gpkg"

def validation_config():
    # vérifs simples
    erreurs = []
    for url in (CONF_VAL_CENIS["wfs_url"], CONF_BAN["wfs_url"], CONF_BATIMENTS.get("catalog_page", "")):
        if not str(url).startswith("http"):
            erreurs.append(f"URL invalide : {url}")

    # test d’écriture
    try:
        testfile = DOSSIER_SORTIE / ".write_test"
        testfile.write_text("ok")
        testfile.unlink(missing_ok=True)
    except Exception as e:
        erreurs.append(f"Impossible d'écrire dans {DOSSIER_SORTIE} : {e}")

    return erreurs


DB_HOST = BASE_DE_DONNEES["host"]
DB_PORT = BASE_DE_DONNEES["port"]
DB_NAME = BASE_DE_DONNEES["database"]
DB_USER = BASE_DE_DONNEES["user"]
DB_PASS = BASE_DE_DONNEES["password"]


if __name__ == "__main__":
    errs = validation_config()
    for e in errs:
        print(f"Erreur : {e}")
