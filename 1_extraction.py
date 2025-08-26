import sys                               
import logging                           
import requests                          
import zipfile                           
import geopandas as gpd                  
from pathlib import Path                 
from urllib.parse import quote           
import re                                

from config import (                     
    LOGGING,
    TEMP_DIR, DATA_DIR,
    CRS_SOURCE, CRS_TARGET,
    VAL_CENIS, BAN, SOMMETS, BATIMENTS
)

# Vérification des dépendances
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
    format=LOGGING["format"],                           
    handlers=[logging.FileHandler(str(LOGGING["file"])),
              logging.StreamHandler(sys.stdout)],       
)
logger = logging.getLogger("extraction")                


class DataExtractor:
    #Extraction des données

    def __init__(self):
        self.temp_dir = TEMP_DIR                                           
        self.data_dir = DATA_DIR                                           
        self.bdtopo_dir = self.temp_dir / BATIMENTS.get("download_subdir", "bdtopo_ign")  
        self.bdtopo_dir.mkdir(parents=True, exist_ok=True)                 
        logger.info("Initialisation extracteur (temp: %s)", self.temp_dir) 

    # Helpers 
    def forcer_2154(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # Reprojeter en 2154 si autre CRS
        if gdf is None or gdf.empty:                                       
            return gdf                                                     
        if str(gdf.crs) != CRS_TARGET:                                     
            return gdf.to_crs(CRS_TARGET)                                  
        return gdf                                                         

    def _gpkg(self, gdf: gpd.GeoDataFrame, filename: str) -> Path:
        # Écrire un GPKG dans data/
        sortie = self.data_dir / filename                                  
        gdf.to_file(sortie, driver="GPKG")                                 
        logger.info("Écrit: %s (%d entités)", sortie, len(gdf))            
        return sortie                                                      

    def wfs_url_1(self, url_base: str, nom_couche: str,
                  cql: str | None = None, max_features: int | None = None) -> str:
        # Construire URL GetFeature
        morceaux = [
            f"{url_base}?service=WFS",                                     
            "version=2.0.0",                                               
            "request=GetFeature",                                          
            f"typeName={nom_couche}",                                      
            f"srsName={CRS_SOURCE}",                                       
        ]
        if cql:                                                            # filtre 
            morceaux.append(f"CQL_FILTER={quote(cql)}")                    
        if max_features:                                                   
            morceaux.append(f"maxFeatures={max_features}")                 
        return "&".join(morceaux)                                          

    def lec_wfs(self, url_base: str, couche: str, cql: str | None = None) -> gpd.GeoDataFrame | None:
        
        try:
            url = self.wfs_url_1(url_base, couche, cql)                    
            logger.info("Lecture WFS: %s", url)                            
            gdf = gpd.read_file(url)                                       
            if gdf.empty:                                                  
                logger.warning("WFS vide pour la couche %s", couche)       
                return None                                                
            return gdf                                                     
        except Exception as e:                                             
            logger.error("Erreur WFS (%s): %s", couche, e)                 
            return None                                                    

    # Val-Cenis / BAN / OSM 
    def recup_vc(self):
        # Commune Val-Cenis via WFS
        logger.info("=== VAL-CENIS ===")                                   
        gdf = self.lec_wfs(VAL_CENIS["wfs_url"], VAL_CENIS["layer_name"], VAL_CENIS["filter"])  
        if gdf is None or gdf.empty:                                       
            logger.error("Val-Cenis introuvable.")                         
            return None                                                    
        gdf = self.forcer_2154(gdf)                                        
        self._gpkg(gdf, "val_cenis_raw.gpkg")                              
        return gdf                                                         

    def recup_ban(self):
        # BAN filtrée sur la commune
        logger.info("=== BAN ===")                                         
        gdf = self.lec_wfs(BAN["wfs_url"], BAN["layer_name"], BAN["filter"])  
        if gdf is None:                                                    
            return None                                                    
        gdf = self.forcer_2154(gdf)                                        
        self._gpkg(gdf, "ban_raw.gpkg")                                    
        return gdf                                                         

    def dl_osm_data(self, val_cenis_gdf=None):
        # Sommets OSM (Geofabrik) + découpage 
        logger.info("=== SOMMETS OSM ===")                                  
        if val_cenis_gdf is None:                                            
            logger.error("Emprise de Val-Cenis requise pour le decoupage.")    
            return None                                                     

        url_zip = SOMMETS["download_url"]                                   
        chemin_zip = self.temp_dir / "osm_rhone_alpes.zip"                  
        dossier_extraction = self.temp_dir / "osm_extracted"                

        try:
            if not chemin_zip.exists():                                     
                logger.info("Téléchargement: %s", url_zip)                  
                with requests.get(url_zip, stream=True, timeout=60) as r:   
                    r.raise_for_status()                                    
                    with open(chemin_zip, "wb") as f:                       
                        for chunk in r.iter_content(1024 * 1024):           
                            if chunk:                                       
                                f.write(chunk)                              
                logger.info("Archive OSM téléchargée: %s", chemin_zip.name) 
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
                logger.error("shp OSM 'natural' introuvable.")              
                return None                                                 

            gdf_nat = gpd.read_file(shp)                                    
            if "fclass" in gdf_nat.columns:                                 
                peaks = gdf_nat[gdf_nat["fclass"] == "peak"].copy()         
            elif "natural" in gdf_nat.columns:                              
                peaks = gdf_nat[gdf_nat["natural"] == "peak"].copy()        
            else:
                peaks = gdf_nat.copy()                                      

            if peaks.empty:                                                 
                logger.warning("Aucun sommet 'peak' trouvé.")  
                return None                                                 

            peaks = self.forcer_2154(peaks)                                 
            vc = self.forcer_2154(val_cenis_gdf)                            
            decoupe = gpd.clip(peaks, vc)                                   

            if decoupe.empty:                                               
                logger.warning("Aucun sommet après application du decoupage par l'emprise de Val-Cenis.")  
                return None                                                 

            self._gpkg(decoupe, "sommets_raw.gpkg")                         
            return decoupe                                                  

        except Exception as e:                                              
            logger.error("Erreur OSM: %s", e)                               
            return None                                                     

    # BD TOPO (bâtiments) 
    def _find_latest_bdtopo_gpkg_link(self) -> str:
        # Trouver l’URL .7z GPKG du D073 sur la page IGN
        if BeautifulSoup is None:                                           
            raise RuntimeError("beautifulsoup4 manquant (pip install beautifulsoup4)")
        page_url = BATIMENTS["catalog_page"]                                
        logger.info("Recherche du lien correct sur: %s", page_url)          

        r = requests.get(page_url, timeout=30)                              
        r.raise_for_status()                                                
        soup = BeautifulSoup(r.text, "html.parser")                         

        pat = re.compile(r"/telechargement/download/BDTOPO/.*GPKG.*D073.*\.7z", re.IGNORECASE)  
        for a in soup.find_all("a", href=True):                             
            href = a["href"]                                                
            if pat.search(href):                                            
                link = href                                                 
                break                                                       
        else:
            link = None                                                     
            for a in soup.find_all("a", href=True):                         
                h = a["href"]                                              
                if "BDTOPO" in h and "GPKG" in h and "D073" in h and h.endswith(".7z"):  
                    link = h; break                                         
        if not link:                                                        
            raise RuntimeError("Lien introuvable sur la page.")             

        if link.startswith("/"):                                          
            link = "https://data.geopf.fr" + link                           
        elif not link.startswith("http"):                                  
            link = f"https://data.geopf.fr/{link.lstrip('./')}"            
        logger.info("Lien détecté: %s", link)                       
        return link                                                         

    def dl_7z(self, url: str, cible: Path):
        # Télécharger archive .7z 
        logger.info("Téléchargement : %s", url)                     
        with requests.get(url, stream=True, timeout=120) as r:            
            r.raise_for_status()                                            
            with open(cible, "wb") as f:                                   
                for chunk in r.iter_content(1024 * 1024):                   
                    if chunk:                                             
                        f.write(chunk)                                     
        logger.info("Archive : %s (%.1f MB)", cible.name, cible.stat().st_size / (1024 * 1024))  

    def _extract_7z(self, chemin_archive: Path, dossier_out: Path):
        # Extraire .7z via py7zr
        if py7zr is None:                                                   
            raise RuntimeError("py7zr manquant (pip install py7zr)")        
        dossier_out.mkdir(parents=True, exist_ok=True)                      
        with py7zr.SevenZipFile(chemin_archive, "r") as z:                  
            z.extractall(path=dossier_out)                                  

    def chercher_gpkg(self, dossier: Path) -> Path | None:
        # Choisir un .gpkg plausible
        candidats = list(dossier.rglob("*.gpkg"))                           
        if not candidats:                                                   
            return None                                                     
        candidats.sort(key=lambda p: (                                      
            "BDTOPO" not in p.name.upper(),
            "D073" not in p.name.upper(),
            -p.stat().st_size
        ))
        return candidats[0]                                                 

    def chercher_couche_bat(self, chemin_gpkg: Path) -> str:
        # Déterminer la couche "batiment"
        couches = fiona.listlayers(chemin_gpkg)                             
        for nom in couches:                                                 
            if BATIMENTS["expected_layer_contains"].lower() in nom.lower(): 
                return nom                                                  
        for nom in couches:                                                 
            if "bati" in nom.lower():                                       
                return nom                                                  
        raise RuntimeError("Couche 'batiment' introuvable.")   

    def extract_batiments_data(self, val_cenis_gdf=None):
        # BD TOPO : téléchargement, lecture, découpage sur Val-Cenis
        logger.info("=== BÂTIMENTS (BD TOPO) ===")                          
        if val_cenis_gdf is None:                                           
            logger.error("Emprise de Val-Cenis requise pour découper les bâtiments.")   
            return None                                                     
        try:
            url = self._find_latest_bdtopo_gpkg_link()                      
        except Exception as e:                                              
            logger.warning("Détection du lien échouée: %s", e)     
            return None                                                     

        chemin_archive = self.bdtopo_dir / Path(url).name                   
        if not chemin_archive.exists():                                     
            self.dl_7z(url, chemin_archive)                                 

        dossier_extrait = self.bdtopo_dir / chemin_archive.stem            
        if not dossier_extrait.exists() or not any(dossier_extrait.iterdir()):  
            self._extract_7z(chemin_archive, dossier_extrait)              

        chemin_gpkg = self.chercher_gpkg(dossier_extrait)                   
        if not chemin_gpkg:                                                 
            logger.error("Aucun .gpkg trouvé dans l'archive BD TOPO.")     
            return None                                                     

        couche_bat = self.chercher_couche_bat(chemin_gpkg)                  
        gdf = gpd.read_file(chemin_gpkg, layer=couche_bat)                 
        if gdf.empty:                                                       
            logger.warning("Couche bâtiments vide.")                        
            return None                                                    

        gdf = self.forcer_2154(gdf)                                         
        vc = self.forcer_2154(val_cenis_gdf)                                
        decoupe = gpd.clip(gdf, vc)                                        
        if decoupe.empty:                                                   
            logger.warning("Aucun bâtiment après le découpage par la commune de Val-Cenis.")  
            return None                                                     

        self._gpkg(decoupe, "batiments_raw.gpkg")                           

        if not BATIMENTS.get("keep_archive", True):                         
            try:
                chemin_archive.unlink(missing_ok=True)                      
            except Exception:
                pass                                                       

        return decoupe                                                      

    # Orchestrateur 
    def extract_all_data(self):
        
        logger.info("=== DÉBUT EXTRACTION ===")                             
        res = {}                                                            
        res["val_cenis"] = self.recup_vc()                                  
        if res["val_cenis"] is None:                                        
            logger.error("Extraction interrompue: Val-Cenis est requis.")   
            return res                                                      

        res["ban"] = self.recup_ban()                                       
        res["sommets"] = self.dl_osm_data(res["val_cenis"])                
        res["batiments"] = self.extract_batiments_data(res["val_cenis"])    

        logger.info("=== RÉSUMÉ ===")                                       
        for k, v in res.items():                                            
            logger.info(" - %-10s : %s", k, f"{len(v)} entités" if v is not None else "échec")  
        logger.info("=== FIN EXTRACTION ===")                                
        return res                                                          

def main():
    extracteur = DataExtractor()                                            
    resultats = extracteur.extract_all_data()                              
    if resultats.get("val_cenis") is None:                                  
        logger.error("ARRÊT: Val-Cenis non extrait.")                      
        return False                                                        

    logger.info("Fichiers générés dans data/:")                              
    for p in DATA_DIR.glob("*.gpkg"):                                       
        try:
            taille_mb = p.stat().st_size / (1024 * 1024)                    
            logger.info("  - %s (%.2f MB)", p.name, taille_mb)              
        except Exception:                                                  
            logger.info("  - %s", p.name)                                  
    return True                                                             

if __name__ == "__main__":
    ok = main()                                                             
    sys.exit(0 if ok else 1)                                                
