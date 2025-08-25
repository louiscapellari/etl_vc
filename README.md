# ETL données géospatiales Val-Cenis 

ETL reproductible pour **Val-Cenis** (Savoie, 73, France) qui : extrait (via WFS et téléchargements OSM et BDTOPO), transforme (nettoyage, formatage, découpage), charge dans une base de donnée PostgreSQL/PostGIS intitulée "etl_vc" et s'exécute via "pipeline.py" ou "run_pipeline_full.bat". Avec la possibilité d'une mise à jour mensuelle via la commande "python pipeline.py --update" ou via le fichier .bat "run_pipeline_update.bat". Pour que le pipeline fonctionne, un environnement python est nécessaire. Les dépendances sont détaillées ci-dessous. 

## Objectifs
- Automatiser l’acquisition de données géospatiales pour la commune de **Val-Cenis** (via utilisation de WFS / téléchargements) ;
- Filtrer/découper par l’emprise de **Val-Cenis** ;
- Créer les tables **PostgreSQL/PostGIS** au bon format et avec les types corrects ;
- Charger automatiquement les attributs et les géométries, indexer, mettre un timestamp, mettre des commentaires ;
- Planifier une **mise à jour mensuelle**.

## Sources des données 
1) Val-Cenis – ADMIN EXPRESS communes mise à jour en continu – IGN<br>
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/administratif.xml<br>
Filtre : nom_officiel = 'Val-Cenis'

2) BAN – Base Adresse Nationale – IGN<br>
Flux WFS : https://data.geopf.fr/annexes/ressources/wfs/adresse.xml<br>
Filtre : nom_commune = 'Val-Cenis'

3) Bâtiments – BD TOPO – GPKG département D073 Savoie – IGN<br>
Page catalogue : https://geoservices.ign.fr/bdtopo<br>
Couche : batiment (détection automatique)<br>
Le script parcourt la page internet pour récupérer le lien .7z le plus récent, télécharge, extrait, puis découpe sur l’emprise Val-Cenis.

4) Sommets – Geofabrik Rhône-Alpes – OSM<br>
Téléchargement : https://download.geofabrik.de/europe/france/rhone-alpes-latest-free.shp.zip<br>
Couche : gis_osm_natural_free_1<br>
Filtre : "peak" (sur champ "fclass")<br>

## Structure 
Base de données : etl_vc

Schéma : vc-etl

Tables :<br> 
val_cenis<br>
Champs : fid, gml_id, cleabs, nom, statut, population, insee_code, date_recensement, insee_canton, insee_arr, insee_dep, siren_code, postal_code, supf_cadas, updated_at<br>
Types : integer(PK), text, text, text, text, text, integer, text, timestamp, text, text text, text, text, integer, timestamp<br>
Géométrie : MULTIPOLYGON, 2154

ban<br>
Champs : Colonnes dynamiques (récupérées du flux)<br>
Types : Similaires aux types utilisés dans le flux.<br> 
Géométrie : POINT, 2154

batiments<br> 
Champs : fid, cleabs, nature, usage1, usage2, construc_legere, etat_obj, date_crea, date_modif, date_apparition, date_confirm, sources, id_sources, methodes_acquis_plani, methode_acquis_alti, precision_plani, precision_alti, nombre_logements, nombre_etages, materiaux_murs, materiaux_toiture, hauteur, alti_mini_sol, alti_mini_toit, alti_max_toit, alti_max_sol, origine_bat, appariement_fonciers, id_rnb, created_at, updated_at<br>
Types : integer(PK), text, text, text, text, bool, text, timestamp, timestamp, date, date, text, text, text, text, float, float, integer, integer, text, float, float, float, float, text, text, text, timestamp, timestamp<br> 
Géométrie : MULTIPOLYGON, 2154 

sommets<br>
Champs : fid, osm_id, nom, altitude, updated_at<br>
Types : integer(PK), text, text, double, timestamp<br>
Géométrie : POINT, 2154

## Structure

**Base de données** : `etl_vc`  
**Schéma** : `vc-etl`

---

### Table : `val_cenis`

| Champs            | Types         | Description (si dispo)   |
|------------------|--------------|---------------------------|
| fid              | integer (PK) | Identifiant unique        |
| gml_id           | text         | ID GML                    |
| cleabs           | text         | Clé ABS                   |
| nom              | text         | Nom                       |
| statut           | text         | Statut                    |
| population       | text         | Population                |
| insee_code       | integer      | Code INSEE                |
| date_recensement | text         | Date du recensement       |
| insee_canton     | text         | Code INSEE du canton      |
| insee_arr        | text         | Code INSEE de l’arr.      |
| insee_dep        | text         | Code INSEE du dép.        |
| siren_code       | text         | Code SIREN                |
| postal_code      | text         | Code postal               |
| supf_cadas       | integer      | Surface cadastrale        |
| updated_at       | timestamp    | Date de mise à jour       |

**Géométrie** : `MULTIPOLYGON` (SRID 2154)

---

### Table : `ban`

| Champs                 | Types       | Notes                             |
|-----------------------|------------|-----------------------------------|
| Colonnes dynamiques   | variables  | Récupérées du flux                |
| *(selon flux)*        | *(similar)*| Types définis par le flux source  |

**Géométrie** : `POINT` (SRID 2154)

---

### Table : `batiments`

| Champs                  | Types         | Description |
|------------------------|--------------|-------------|
| fid                    | integer (PK) | Identifiant unique |
| cleabs                 | text         | Identifiant/clé de l’objet dans la source |
| nature                 | text         | Nature du bâtiment |
| usage1                 | text         | Usage principal |
| usage2                 | text         | Usage secondaire |
| construc_legere        | bool         | Indique une construction légère |
| etat_obj               | text         | État de l’objet |
| date_crea              | timestamp    | Date/heure de création de l’enregistrement |
| date_modif             | timestamp    | Date/heure de dernière modification |
| date_apparition        | date         | Date d’apparition dans la source |
| date_confirm           | date         | Date de confirmation/validation |
| sources                | text         | Libellé(s) de la/les source(s) de données |
| id_sources             | text         | Identifiant(s) de la/les source(s) |
| methodes_acquis_plani  | text         | Méthode d’acquisition planimétrique |
| methode_acquis_alti    | text         | Méthode d’acquisition altimétrique |
| precision_plani        | float        | Précision planimétrique (m) |
| precision_alti         | float        | Précision altimétrique (m) |
| nombre_logements       | integer      | Nombre de logements |
| nombre_etages          | integer      | Nombre d’étages |
| materiaux_murs         | text         | Matériaux des murs |
| materiaux_toiture      | text         | Matériaux de la toiture |
| hauteur                | float        | Hauteur du bâtiment (m) |
| alti_mini_sol          | float        | Altitude minimale au sol (m) |
| alti_mini_toit         | float        | Altitude minimale du toit (m) |
| alti_max_toit          | float        | Altitude maximale du toit (m) |
| alti_max_sol           | float        | Altitude maximale au sol (m) |
| origine_bat            | text         | Origine (numérisé, import externe, etc.) |
| appariement_fonciers   | text         | Statut d’appariement avec les données foncières |
| id_rnb                 | text         | Identifiant au Référentiel National des Bâtiments (RNB) |
| created_at             | timestamp    | Date de création côté ETL |
| updated_at             | timestamp    | Date de mise à jour côté ETL |

**Géométrie** : `MULTIPOLYGON` (SRID 2154)

---

### Table : `sommets`

| Champs      | Types         | Description |
|------------|--------------|-------------|
| fid        | integer (PK) | Identifiant unique du sommet |
| osm_id     | text         | Identifiant OpenStreetMap |
| nom        | text         | Nom du sommet |
| altitude   | double       | Altitude (mètres) |
| updated_at | timestamp    | Date de mise à jour |

**Géométrie** : `POINT` (SRID 2154)


## Dépendances 
Testé dans un environnement python 3.12.11 généré via Anaconda.
Testé avec PostgreSQL 17. 

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
 3. Exécuter le fichier .bat "run_pipeline_full.bat" (il faut changer les chemins à l'intérieur au préalable), ou dans un terminal exploitant l'environnement python la commande "python pipeline.py --full" (uniquement lorsque vous êtes placé dans le dossier contenant les scripts);
 4. Le script va exécuter le processus ETL automatiquement jusqu'à sa complétion ;
 5. Une fois terminé, la base de données "etl_vc" sera alimentée, les données seront stockées dans le schéma "vc_etl" ;
 6. Pour mettre à jour les données, lancer le fichier .bat "run_pipeline_upadate.bat", ou dans un terminal exploitant l'environnement python la commande "python pipeline.py --update". La mise à jour ne s'effectuera que si le "run_pipeline_full.bat" a été exécuté il y a plus de trente jours. Si vous souhaitez mettre à jour avant les trente jours, il faut exécuter le "run_pipeline_full.bat" ou la commande "python pipeline.py --full" à nouveau.
