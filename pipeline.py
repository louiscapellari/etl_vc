# Orchestrateur principal 

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
    BASE_DIR, OUTPUT_DIR, LOG_DIR,
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
journal = logging.getLogger(__name__)        

# Constantes
FICHIER_ETAT = LOG_DIR / "update_state.json" # fichier d’état
DOSSIER_SAUV = BASE_DIR / "backups"          # dossier backups
DOSSIER_SAUV.mkdir(exist_ok=True)            # crée si absent
PYTHON_EXE = sys.executable                  # interpréteur courant

SCRIPTS = {                                  # chemins des scripts à enchaîner
    "bdd": BASE_DIR / "2_bdd.py",
    "extract": BASE_DIR / "1_extraction.py",
    "etl": BASE_DIR / "3_etl.py",
}

# Utils
def lancer_sous_processus(nom: str, chemin_script: Path) -> bool:
    
    if not chemin_script.exists():                          
        journal.error("Script introuvable: %s", chemin_script)
        return False
    journal.info("Lancement %s: %s", nom, chemin_script.name)
    try:
        ret = subprocess.run([PYTHON_EXE, str(chemin_script)], check=False)  
        if ret.returncode == 0:                              
            journal.info("%s terminé avec succès", nom)
            return True
        journal.error("%s a échoué (code %s)", nom, ret.returncode)         
        return False
    except Exception as e:                                   
        journal.error("Erreur lors de l’exécution de %s: %s", nom, e)
        return False

def lire_etat() -> dict:
    
    if not FICHIER_ETAT.exists():                           
        return {}
    try:
        return json.loads(FICHIER_ETAT.read_text(encoding="utf-8"))  
    except Exception:
        return {}                                           

def ecrire_etat(etat: dict) -> None:
    
    try:
        FICHIER_ETAT.write_text(                            # dump
            json.dumps(etat, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        journal.warning("Impossible d’écrire le fichier d’état: %s", e)     

def derniere_date_maj():
    
    ts = lire_etat().get("last_update_utc")                 
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
        journal.info("Backups purgés: %s élément(s) supprimé(s) (> %sj).", suppr, jours)

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
    journal.info("Sauvegarde BDD (pg_dump)...")
    env = dict(os.environ, PGPASSWORD=DATABASE["password"])             
    try:
        ret = subprocess.run(cmd, check=False, env=env)                 
        if ret.returncode == 0:
            journal.info("Dump PG créé: %s", chemin_dump.name)
        else:
            journal.warning("Échec pg_dump (vérifier installation/PATH des outils PostgreSQL).")
    except FileNotFoundError:
        journal.warning("pg_dump introuvable (installez les outils PostgreSQL / PATH).")
    except Exception as e:
        journal.warning("Erreur pg_dump: %s", e)

    
    chemin_zip = dest / f"exports_gpkg_{horo}.zip"                      
    journal.info("Archivage des GPKG...")
    try:
        with zipfile.ZipFile(chemin_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for gpkg in OUTPUT_DIR.glob("*.gpkg"):                      
                z.write(gpkg, arcname=gpkg.name)                        
        journal.info("Archive GPKG créée: %s", chemin_zip.name)
    except Exception as e:
        journal.warning("Erreur lors de la création de l’archive GPKG: %s", e)

    return dest                                                         

# Orchestration

def executer_pipeline(args) -> int:
    
    journal.info("=" * 70)
    journal.info("DÉMARRAGE PIPELINE ETL - Val-Cenis")
    journal.info("Base: %s | Host: %s | Port: %s",
                 DATABASE['database'], DATABASE['host'], DATABASE['port'])
    journal.info("=" * 70)

    purger_anciennes_sauvegardes()                                      

    # scénario d'exécution 
    lancer_bdd = not args.etl_only                                      
    lancer_extraction = not args.etl_only                               
    lancer_etl = not args.extract_only                                   
    faire_backup = UPDATE_CONFIG.get("backup_before_update", True) and (not args.no_backup)  

    if args.full:                                                        
        besoin_maj = True
        journal.info("Mode FULL : toutes les étapes seront exécutées.")
    elif args.update:                                                    
        besoin_maj = maj_mensuelle_necessaire()
        journal.info("Mode UPDATE : %s", "mise à jour requise" if besoin_maj else "déjà à jour (<30j)")
        if not besoin_maj:
            journal.info("Rien à faire (utiliser --full pour forcer).")
            return 0
    else:                                                                
        besoin_maj = maj_mensuelle_necessaire()
        journal.info("Mode automatique : %s", "mise à jour nécessaire" if besoin_maj else "déjà à jour")
        if not besoin_maj:
            journal.info("Rien à faire (utiliser --update ou --full pour forcer).")
            return 0

    if faire_backup:                                                     
        sauvegarde_complete()

    
    if lancer_bdd and not lancer_sous_processus("Création BDD/Schéma", SCRIPTS["bdd"]):
        journal.error("Arrêt : échec lors de la création du schéma/BDD.")
        return 1

    if lancer_extraction and not lancer_sous_processus("Extraction", SCRIPTS["extract"]):
        journal.error("Arrêt : échec durant l’extraction.")
        return 1

    if lancer_etl and not lancer_sous_processus("ETL (Transformation + Chargement)", SCRIPTS["etl"]):
        journal.error("Arrêt : échec durant l’ETL.")
        return 1

    # Mise à jour de l’état 
    etat = lire_etat()                                                   
    etat["last_update_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")  
    ecrire_etat(etat)                                                    

    journal.info("PIPELINE TERMINÉ AVEC SUCCÈS")                         
    return 0                                                             


def construire_parser():
    
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
    
    args = construire_parser().parse_args()                              
    try:
        return executer_pipeline(args)                                   
    except KeyboardInterrupt:
        journal.warning("Interruption utilisateur.")                     
        return 1
    except Exception as e:
        journal.error("Erreur fatale orchestrateur: %s", e)              
        return 1

if __name__ == "__main__":                                               
    sys.exit(main())                                                     
