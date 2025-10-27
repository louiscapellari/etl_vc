import sys
import os
import argparse
import subprocess
import shutil
import json
import zipfile
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import (
    DOSSIER_BASE, DOSSIER_SORTIE, DOSSIER_LOGS,
    DATABASE, UPDATE_CONFIG, LOGGING
)


# logs
logging.basicConfig(
    level=getattr(logging, LOGGING['level']),
    format=LOGGING['format'],
    handlers=[
        logging.FileHandler(LOGGING['file']),
        logging.StreamHandler(sys.stdout)
    ]
)
logs = logging.getLogger(__name__)

# Constantes
FICHIER_ETAT = DOSSIER_LOGS / "update_state.json"  # fichier d’état
DOSSIER_SAUV = DOSSIER_BASE / "backups"           # dossier backups
DOSSIER_SAUV.mkdir(exist_ok=True)             
PYTHON_EXE = sys.executable                   # interpréteur

SCRIPTS = {                                   # chemins des scripts à enchaîner
    "bdd": DOSSIER_BASE / "2_bdd.py",
    "extract": DOSSIER_BASE / "1_extraction.py",
    "etl": DOSSIER_BASE / "3_etl.py",
}


def lancer_sous_processus(nom: str, chemin_script: Path) -> bool:
    if not chemin_script.exists():
        logs.error(f"Erreur : script introuvable ({chemin_script}).")
        return False
    logs.info(f"Lancement : {nom}.")
    try:
        ret = subprocess.run([PYTHON_EXE, str(chemin_script)], check=False)
        if ret.returncode == 0:
            logs.info(f"{nom} terminé.")
            return True
        logs.error(f"Erreur : {nom} (code {ret.returncode}).")
        return False
    except Exception as e:
        logs.error(f"Erreur : exécution de {nom} — {e}")
        return False

def read_state() -> dict:
    if not FICHIER_ETAT.exists():
        return {}
    try:
        return json.loads(FICHIER_ETAT.read_text(encoding="utf-8"))
    except Exception:
        return {}

def write_state(etat: dict) -> None:
    try:
        FICHIER_ETAT.write_text(
            json.dumps(etat, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        logs.warning(f"Impossible d’écrire le fichier d’état : {e}")

def derniere_date_maj():
    ts = read_state().get("last_update_utc")
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def maj_mensuelle_necessaire() -> bool:
    derniere = derniere_date_maj()
    if derniere is None:
        return True
    delta = datetime.now(timezone.utc) - derniere.replace(tzinfo=timezone.utc)
    return delta.days >= 30

def purger_anciennes_sauvegardes() -> None:
    jours = int(UPDATE_CONFIG.get("keep_backup_days", 90))
    limite = datetime.now(timezone.utc) - timedelta(days=jours)

    suppr = 0
    for item in DOSSIER_SAUV.glob("*"):
        try:
            mtime = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc)
            if mtime < limite:
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
                suppr += 1
        except Exception:
            continue
    if suppr:
        logs.info(f"Purge des sauvegardes : {suppr} élément(s) supprimé(s).")

def sauvegarde_complete() -> Path:
    horo = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest = DOSSIER_SAUV / f"backup_{horo}"
    dest.mkdir(exist_ok=True)

    # Dump Postgres
    chemin_dump = dest / f"{DATABASE['database']}_vc_etl_{horo}.dump"
    cmd = [
        "pg_dump",
        "-h", DATABASE["host"],
        "-p", str(DATABASE["port"]),
        "-U", DATABASE["user"],
        "-n", "vc_etl",
        "-Fc",
        "-f", str(chemin_dump),
        DATABASE["database"],
    ]
    logs.info("Sauvegarde de la base…")
    env = dict(os.environ, PGPASSWORD=DATABASE["password"])
    try:
        ret = subprocess.run(cmd, check=False, env=env)
        if ret.returncode == 0:
            logs.info(f"Dump créé : {chemin_dump.name}.")
        else:
            logs.warning("pg_dump non exécuté.")
    except FileNotFoundError:
        logs.warning("pg_dump introuvable.")
    except Exception as e:
        logs.warning(f"pg_dump : {e}")

    # Zip des exports GPKG
    chemin_zip = dest / f"exports_gpkg_{horo}.zip"
    logs.info("Archivage des exports (GPKG).")
    try:
        with zipfile.ZipFile(chemin_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for gpkg in DOSSIER_SORTIE.glob("*.gpkg"):
                z.write(gpkg, arcname=gpkg.name)
        logs.info(f"Archive créée : {chemin_zip.name}.")
    except Exception as e:
        logs.warning(f"Archive GPKG : {e}")

    return dest

# Orchestration
def executer_pipeline(args) -> int:
    logs.info("Début du pipeline.")
    purger_anciennes_sauvegardes()

    lancer_bdd = not args.etl_only
    lancer_extraction = not args.etl_only
    lancer_etl = not args.extract_only
    faire_backup = UPDATE_CONFIG.get("backup_before_update", True) and (not args.no_backup)

    if args.full:
        besoin_maj = True
        logs.info("Mode : exécution complète.")
    elif args.update:
        besoin_maj = maj_mensuelle_necessaire()
        logs.info("Mode : mise à jour.")
        if not besoin_maj:
            logs.info("Déjà à jour (moins de 30 jours).")
            return 0
    else:
        besoin_maj = maj_mensuelle_necessaire()
        logs.info("Mode : automatique.")
        if not besoin_maj:
            logs.info("Rien à faire (utiliser --update ou --full pour forcer).")
            return 0
    # enchaînement des scripts
    if lancer_bdd and not lancer_sous_processus("Création BDD/Schéma", SCRIPTS["bdd"]):
        logs.error("Erreur : création du schéma/base.")
        return 1

    if lancer_extraction and not lancer_sous_processus("Extraction", SCRIPTS["extract"]):
        logs.error("Erreur : extraction.")
        return 1

    if lancer_etl and not lancer_sous_processus("ETL (Transformation + Chargement)", SCRIPTS["etl"]):
        logs.error("Erreur : ETL.")
        return 1

    etat = read_state()
    etat["last_update_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_state(etat)

    logs.info("Pipeline ETL terminé.")
    return 0


def build_parser():
    p = argparse.ArgumentParser(
        description="Orchestrateur principal du pipeline ETL Val-Cenis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--update", action="store_true",
                   help="Lancer une mise à jour si > 30 jours depuis la dernière.")
    g.add_argument("--full", action="store_true",
                   help="Tout forcer (backup + bdd + extraction + etl).")
    g.add_argument("--extract-only", action="store_true",
                   help="Exécuter uniquement l’extraction.")
    g.add_argument("--etl-only", action="store_true",
                   help="Exécuter uniquement l’ETL.")
    p.add_argument("--no-backup", action="store_true",
                   help="Ne pas faire de sauvegarde avant l’exécution.")
    return p

def main():
    args = build_parser().parse_args()
    try:
        return executer_pipeline(args)
    except KeyboardInterrupt:
        logs.warning("Interruption utilisateur.")
        return 1
    except Exception as e:
        logs.error(f"Erreur : orchestrateur — {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
