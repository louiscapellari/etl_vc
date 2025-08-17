# ETL données géospatiales Val-Cenis 

ETL reproductible pour Val-Cenis (Savoie, 73, France) qui : extrait (WFS/OSM/BDTOPO), transforme (nettoyage, formatage, découpage), charge dans une base de donnée PostgreSQL/PostGIS intitulée "etl_vc" et s'exécute via "pipeline.py" ou "run_pipeline_full.bat". Avec la possibilité d'une mise à jour mensuelle via la commande "python pipeline.py --update" dans un environnement python ou via le fichier .bat "run_pipeline_update.bat". 

## Objectifs
- Automatiser l’acquisition de données géospatiales (via utilisation du WFS / téléchargement) ;
- Filtrer/découper par l’emprise de **Val-Cenis** ;
- Créer les tables **PostgreSQL/PostGIS** au bon format ;
- Charger les attributs et les géométries, indexer, timestamp, commentaires ;
- Planifier une **mise à jour mensuelle**.

## Sources des données 
1) Val-Cenis (ADMIN EXPRESS (mise à jour en continu) – communes, IGN WFS)<br>
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/administratif.xml<br>
Filtre : nom_officiel = 'Val-Cenis'

2) BAN – Base Adresse Nationale (IGN WFS)<br>
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/adresse.xml<br>
Filtre : nom_commune = 'Val-Cenis'

3) Bâtiments (BD TOPO IGN – GPKG département D073 Savoie)<br>
Page catalogue : https://geoservices.ign.fr/bdtopo<br>
Couche : batiment (détection automatique)<br>
Le script parcourt la page internet pour récupérer le lien .7z le plus récent, télécharge, extrait, puis découpe sur l’emprise Val-Cenis.

4) Sommets (OSM – Geofabrik Rhône-Alpes)<br>
Téléchargement : https://download.geofabrik.de/europe/france/rhone-alpes-latest-free.shp.zip<br>
Couche : gis_osm_natural_free_1<br>
Filtre : "peak" (sur champ "fclass")<br>
Le script filtre sur la couche, extrait, puis découpe sur l'emprise de Val-Cenis.

## Structure 
Schéma = "vc-etl" 

Tables :<br> 
val_cenis<br>
Champs : fid, gml_id, cleabs, nom, statut, population, insee_code, date_recensement, insee_canton, insee_arr, insee_dep, siren_code, postal_code, supf_cadas, updated_at<br>
Types : integer (PK), text, text, text, text, text, integer, text, timestamp, text, text text, text, text, integer, timestamp<br>
Géométrie : MULTIPOLYGON, 2154

ban<br>
Champs : Colonnes dynamiques (récupérées du flux)<br>
Types : Similaires aux types utilisés dans le flux.<br> 
Géométrie : POINT, 2154

batiments<br> 
Champs : fid, cleabs, nature, usage1, usage2, construc_legere, etat_obj, date_crea, date_modif, date_apparition, date_confirm, sources, id_sources, methodes_acquis_plani, methode_acquis_alti, precision_plani, precision_alti, nombre_logements, nombre_etages, materiaux_murs, materiaux_toiture, hauteur, alti_mini_sol, alti_mini_toit, alti_max_toit, alti_max_sol, origine_bat, appariement_fonciers, id_rnb, created_at, updated_at<br>
Types : integer, text, text, text, text, bool, text, timestamp, timestamp, date, date, text, text, text, text, float, float, integer, integer, text, float, float, float, float, text, text, text, timestamp, timestamp<br> 
Géométrie : MULTIPOLYGON, 2154 

sommets<br>
Champs : fid, osm_id, nom, altitude, updated_at<br>
Types : PK, TEXT, TEXT, DOUBLE, DateTime<br>
Géométrie : POINT, 2154

## Dépendances 
Environnement Anaconda ou Miniconda en python>3.12 recommandé.

Liste des librairies python indispensables :<br> 
- geopandas
- pandas
- requests
- fiona
- shapely
- psycopg2
- psycopg2-binary
- SQLAlchemy
- py7zr
- beautifulsoup4

## Instructions 
 1. Créer un environnement python disposant de toutes les librairies mentionnées ci-dessus ;
 2. Créer une base de données PostgreSQL/PostGIS nommée "etl_vc" ;
 3. Lancer le fichier .bat "run_pipeline_full.bat" ou dans le terminal de l'environnement python la commande "python pipeline.py --full";
 4. Le script va exécuter le processus ETL automatiquement jusqu'à sa complétion ;
 5. Une fois terminé, la base de données "etl_vc" sera alimentée, les données seront stockées dans le schéma "vc_etl" ;
 6. Pour mettre à jour les données, lancer le fichier .bat "run_pipeline_upadate.bat". La mise à jour ne s'effectuera que si le "run_pipeline_full.bat" a été exécuté il y a plus de trente jours. Si vous souhaitez mettre à jour avant les trente jours, il faut exécuter le "run_pipeline_full.bat" à nouveau.
