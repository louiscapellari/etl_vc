# ETL données géospatiales Val-Cenis 

ETL reproductible pour Val-Cenis (Savoie, 73, France) qui : extrait (WFS/OSM/BDTOPO), transforme (nettoyage + mapping), charge dans une base de donnée PostgreSQL/PostGIS intitulée "etl_vc" et s'exécute via pipeline.py ou run_pipeline_full.bat. Avec la possibilité d'une mise à jour mensuelle via la commande "python pipeline.py --update" dans un environnement python ou utiliser le run_pipeline_update.bat. 

## Objectifs
- Automatiser l’acquisition de données géospatiales (WFS / téléchargement) ;
- Filtrer/découper par l’emprise de **Val-Cenis** ;
- Créer les tables **PostgreSQL/PostGIS** au bon format ;
- Charger les attributs + géométries, indexer, timestamp, commentaires ;
- Planifier une **mise à jour mensuelle** (état + backup).

## Sources des données 
1) Val-Cenis (ADMIN EXPRESS – communes, IGN WFS)
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/administratif.xml
filtre : nom_officiel = 'Val-Cenis'

2) BAN – Base Adresse Nationale (IGN WFS)
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/adresse.xml
filtre : nom_commune = 'Val-Cenis'

3) Bâtiments (BD TOPO IGN – GPKG département D073 Savoie)
Page catalogue : https://geoservices.ign.fr/bdtopo
Couche : batiment (détection automatique)
Le script parcourt la page pour récupérer le lien .7z le plus récent, télécharge, extrait, puis découpe sur l’emprise Val-Cenis.

5) Sommets (OSM – Geofabrik Rhône-Alpes)
Téléchargement : https://download.geofabrik.de/europe/france/rhone-alpes-latest-free.shp.zip
Couche : gis_osm_natural_free_1
Filtre : "peak" (sur champ "fclass")
Le script filtre sur la couche, extrait, puis découpe sur l'emprise de Val-Cenis

## Structure 
Schéma = "vc-etl" 

Tables : 
val_cenis
Champs : fid, gml_id, cleabs, nom, statut, population, insee_code, date_recensement, insee_canton, insee_arr, insee_dep, siren_code, postal_code, supf_cadas, updated_at
Types : PK, String, String, String, String, String, Integer, String, DateTime, String, String, String, String, String, Integer, DateTime
Géométrie : geom geometry(MULTIPOLYGON, 2154)

ban
Champs : Colonnes dynamiques (récupérées du flux)
Types : Similaires aux types utilisés dans le flux. 
Géométrie : geom geometry(POINT, 2154)

sommets
Champs : fid, osm_id, nom, altitude, updated_at
Types : PK, TEXT, TEXT, DOUBLE, DateTime
Géométrie : geom geometry(POINT, 2154)

## Dépendances 

Environnement Anaconda ou Miniconda recommandé. 

Liste des librairies indispensables : 
geopandas
pandas
requests
fiona
shapely
psycopg2-binary
SQLAlchemy
py7zr
beautifulsoup4



