import sys
import logging
import requests
import zipfile
import geopandas as gpd
from pathlib import Path
from urllib.parse import quote
import re
import shutil  

from config import (
    LOGGING,
    DOSSIER_TEMP, DOSSIER_DONNEES,
    CRS_ENTREE, CRS_CIBLE,
    VAL_CENIS, BAN, SOMMETS, BATIMENTS
)

# Vérification des dépendances python 
try:
    import py7zr
except ImportError:
    py7zr = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

import fiona

# logs 
logging.basicConfig(
    level=getattr(logging, LOGGING["level"]),
    format=LOGGING["format"],  # "%(message)s"
    handlers=[
        logging.FileHandler(str(LOGGING["file"])),
        logging.StreamHandler(sys.stdout)
    ],
)
logger = logging.getLogger("extraction")


class DataExtractor:
    # Extraction des données

    def __init__(self):
        self.temp_dir = DOSSIER_TEMP
        self.data_dir = DOSSIER_DONNEES
        self.bdtopo_dir = self.temp_dir / BATIMENTS.get("download_subdir", "bdtopo_ign")
        self.bdtopo_dir.mkdir(parents=True, exist_ok=True)
        logger.info("__init__ialisation de l'extraction.")

    
    def forcer_2154(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Reprojeter en 2154 si autre CRS
        if gdf is None or gdf.empty:
            return gdf
        if str(gdf.crs) != CRS_CIBLE:
            return gdf.to_crs(CRS_CIBLE)
        return gdf

    def _gpkg(self, gdf: gpd.GeoDataFrame, filename: str) -> Path:
        sortie = self.data_dir / filename
        gdf.to_file(sortie, driver="GPKG")
        logger.info(f"Fichier écrit : {sortie.name} ({len(gdf)} lignes).")
        return sortie

    def wfs_url_1(self, url_base: str, nom_couche: str,
                  cql: str | None = None, max_features: int | None = None) -> str:
        # URL GetFeature
        morceaux = [
            f"{url_base}?service=WFS",
            "version=2.0.0",
            "request=GetFeature",
            f"typeName={nom_couche}",
            f"srsName={CRS_ENTREE}",
        ]
        if cql:  # filtre
            morceaux.append(f"CQL_FILTER={quote(cql)}")
        if max_features:
            morceaux.append(f"maxFeatures={max_features}")
        return "&".join(morceaux)

    def lec_wfs(self, url_base: str, couche: str, cql: str | None = None) -> gpd.GeoDataFrame | None:
        # Lecture WFS
        try:
            url = self.wfs_url_1(url_base, couche, cql)
            logger.info(f"WFS : {couche}")
            gdf = gpd.read_file(url)
            if gdf.empty:
                logger.info(f"WFS vide : {couche}.")
                return None
            return gdf
        except Exception as e:
            logger.error(f"Erreur : WFS {couche} — {e}")
            return None

    # Val-Cenis / BAN / OSM
    def recup_vc(self):
        # Commune Val-Cenis via WFS
        logger.info("Val-Cenis.")
        gdf = self.lec_wfs(VAL_CENIS["wfs_url"], VAL_CENIS["layer_name"], VAL_CENIS["filter"])
        if gdf is None or gdf.empty:
            logger.error("Erreur : Val-Cenis introuvable.")
            return None
        gdf = self.forcer_2154(gdf)
        self._gpkg(gdf, "val_cenis_raw.gpkg")
        return gdf

    def recup_ban(self):
        # BAN filtrée sur la commune
        logger.info("BAN.")
        gdf = self.lec_wfs(BAN["wfs_url"], BAN["layer_name"], BAN["filter"])
        if gdf is None:
            return None
        gdf = self.forcer_2154(gdf)
        self._gpkg(gdf, "ban_raw.gpkg")
        return gdf

    def dl_osm_data(self, val_cenis_gdf=None):
        # Sommets OSM + découpage
        logger.info("Sommets OSM.")
        if val_cenis_gdf is None:
            logger.error("Erreur : emprise de Val-Cenis requise pour le découpage.")
            return None

        url_zip = SOMMETS["download_url"]
        chemin_zip = self.temp_dir / "osm_rhone_alpes.zip"
        dossier_extraction = self.temp_dir / "osm_extracted"

        try:
            if not chemin_zip.exists():
                logger.info("Téléchargement OSM.")
                with requests.get(url_zip, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    with open(chemin_zip, "wb") as f:
                        for chunk in r.iter_content(1024 * 1024):
                            if chunk:
                                f.write(chunk)
                logger.info("Archive OSM téléchargée.")
            if not dossier_extraction.exists():
                with zipfile.ZipFile(chemin_zip, "r") as z:
                    z.extractall(dossier_extraction)

            shp = None
            for p in dossier_extraction.rglob("*.shp"):
                if "gis_osm_natural_free_1" in p.name:
                    shp = p; break
            if shp is None:
                for p in dossier_extraction.rglob("*natural*.shp"):
                    shp = p; break
            if shp is None:
                logger.error("Erreur : shapefile OSM introuvable.")
                return None

            gdf_nat = gpd.read_file(shp)
            if "fclass" in gdf_nat.columns:
                peaks = gdf_nat[gdf_nat["fclass"] == "peak"].copy()
            elif "natural" in gdf_nat.columns:
                peaks = gdf_nat[gdf_nat["natural"] == "peak"].copy()
            else:
                peaks = gdf_nat.copy()

            if peaks.empty:
                logger.info("Aucun sommet trouvé.")
                return None

            peaks = self.forcer_2154(peaks)
            vc = self.forcer_2154(val_cenis_gdf)
            decoupe = gpd.clip(peaks, vc)

            if decoupe.empty:
                logger.info("Aucun sommet après découpage.")
                return None

            self._gpkg(decoupe, "sommets_raw.gpkg")
            return decoupe

        except Exception as e:
            logger.error(f"Erreur : OSM — {e}")
            return None

    # BD TOPO (couche des bâtiments)
    def trouver_lien_bdtopo(self) -> str:
        # Trouver l’URL .7z GPKG du D073 sur la page IGN
        if BeautifulSoup is None:
            raise RuntimeError("beautifulsoup4 manquant (pip install beautifulsoup4)")
        page_url = BATIMENTS["catalog_page"]
        logger.info("Recherche du lien BD TOPO.")

        r = requests.get(page_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        pat = re.compile(r"/telechargement/download/BDTOPO/.*GPKG.*D073.*\.7z", re.IGNORECASE)
        link = None
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if pat.search(href):
                link = href
                break
        if not link:
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "BDTOPO" in h and "GPKG" in h and "D073" in h and h.endswith(".7z"):
                    link = h; break
        if not link:
            raise RuntimeError("lien BD TOPO introuvable.")

        if link.startswith("/"):
            link = "https://data.geopf.fr" + link
        elif not link.startswith("http"):
            link = f"https://data.geopf.fr/{link.lstrip('./')}"
        logger.info("Lien trouvé.")
        return link

    def dl_7z(self, url: str, cible: Path):
        # Télécharger archive .7z
        logger.info("Téléchargement BD TOPO.")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(cible, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
        logger.info("Archive BD TOPO téléchargée.")

    def cleanup_anciens_dossiers(self, archive_stem: str):
        try:
            for p in self.bdtopo_dir.iterdir():
                if p.is_dir() and archive_stem in p.name:
                    shutil.rmtree(p, ignore_errors=True)
        except Exception:
            pass

    def extract_gpkg_only(self, chemin_archive: Path, dossier_out: Path) -> Path:
        # Extraire uniquement le .gpkg depuis l'archive .7z 
        if py7zr is None:
            raise RuntimeError("py7zr manquant (pip install py7zr)")

        dossier_out.mkdir(parents=True, exist_ok=True)
        logger.info("Extraction du GPKG.")

        with py7zr.SevenZipFile(str(chemin_archive), mode="r") as z:
            names = z.getnames()
            candidats = [n for n in names if n.lower().endswith(".gpkg")]
            if not candidats:
                raise RuntimeError("aucun GPKG trouvé dans l'archive BD TOPO.")
            
            candidats.sort(key=lambda n: ("d073" not in n.lower(), len(n)))
            cible = candidats[0]

            data_map = z.read([cible]) 
            bio = data_map[cible]
            sortie = dossier_out / Path(cible).name
            with open(sortie, "wb") as f:
                f.write(bio.getbuffer())

        logger.info(f"GPKG extrait : {sortie.name}.")
        return sortie

    def chercher_couche_bat(self, chemin_gpkg: Path) -> str:
        # Déterminer la couche "batiment"
        couches = fiona.listlayers(chemin_gpkg)
        for nom in couches:
            if BATIMENTS["expected_layer_contains"].lower() in nom.lower():
                return nom
        for nom in couches:
            if "bati" in nom.lower():
                return nom
        raise RuntimeError("couche 'batiment' introuvable.")

    def extract_batiments_data(self, val_cenis_gdf=None):
        
        logger.info("Bâtiments.")
        if val_cenis_gdf is None:
            logger.error("Erreur : emprise Val-Cenis requise pour les bâtiments.")
            return None
        try:
            url = self.trouver_lien_bdtopo()
        except Exception as e:
            logger.error(f"Erreur : détection lien BD TOPO — {e}")
            return None

        chemin_archive = self.bdtopo_dir / Path(url).name
        if not chemin_archive.exists():
            self.dl_7z(url, chemin_archive)

        self.cleanup_anciens_dossiers(chemin_archive.stem)

        try:
            chemin_gpkg = self.extract_gpkg_only(chemin_archive, self.bdtopo_dir)
        except Exception as e:
            logger.error(f"Erreur : extraction GPKG — {e}")
            return None

        # Lecture + reprojection + découpage
        try:
            couche_bat = self.chercher_couche_bat(chemin_gpkg)
            gdf = gpd.read_file(chemin_gpkg, layer=couche_bat)
            if gdf.empty:
                logger.info("Couche bâtiments vide.")
                return None

            gdf = self.forcer_2154(gdf)
            vc = self.forcer_2154(val_cenis_gdf)
            decoupe = gpd.clip(gdf, vc)
            if decoupe.empty:
                logger.info("Aucun bâtiment après découpage.")
                return None

            self._gpkg(decoupe, "batiments_raw.gpkg")

            if not BATIMENTS.get("keep_archive", True):
                try:
                    chemin_archive.unlink(missing_ok=True)
                except Exception:
                    pass

            return decoupe

        except Exception as e:
            logger.error(f"Erreur : traitement batiments — {e}")
            return None

    
    def extract_all_data(self):
        logger.info("Début extraction.")
        res = {}
        res["val_cenis"] = self.recup_vc()
        if res["val_cenis"] is None:
            logger.error("Erreur : Val-Cenis requis.")
            return res

        res["ban"] = self.recup_ban()
        res["sommets"] = self.dl_osm_data(res["val_cenis"])
        res["batiments"] = self.extract_batiments_data(res["val_cenis"])

        logger.info("Extraction terminée.")
        return res


def main():
    extracteur = DataExtractor()
    resultats = extracteur.extract_all_data()
    if resultats.get("val_cenis") is None:
        logger.error("Erreur : extraction interrompue (Val-Cenis).")
        return False

    return True


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
