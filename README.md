# ETL données géospatiales Val-Cenis 

ETL reproductible pour **Val-Cenis** (Savoie, 73, France) qui : extrait (via WFS et téléchargements OSM et BDTOPO), transforme (nettoyage, formatage, découpage), charge dans une base de donnée PostgreSQL/PostGIS intitulée "etl_vc" et s'exécute via "pipeline.py" ou "run_pipeline_full.bat". Avec la possibilité d'une mise à jour mensuelle via la commande "python pipeline.py --update" ou via le fichier .bat "run_pipeline_update.bat". Pour que le pipeline fonctionne, un environnement python est nécessaire. Les dépendances sont détaillées ci-dessous. 

## Objectifs
- Automatiser l’acquisition de données géospatiales pour la commune de **Val-Cenis** (via utilisation de WFS / téléchargements) ;
- Filtrer/découper par l’emprise de **Val-Cenis** ;
- Créer les tables **PostgreSQL/PostGIS** au bon format et avec les types corrects ;
- Charger automatiquement les attributs et les géométries, indexer, mettre un timestamp, mettre des commentaires ;
- Planifier une **mise à jour mensuelle**.

## Sources des données

### 1) Val-Cenis – ADMIN EXPRESS (IGN)
|        |                                                                 |
|-----------------|------------------------------------------------------------------------|
| **Nom du flux** | ADMIN EXPRESS mises a jour en continu - commune                    |
| **Fournisseur**    | IGN                                                                    |
| **Accès (WFS)**    | https://data.geopf.fr/annexes/ressources/wfs/administratif.xml         |
| **Filtre appliqué**| `nom_officiel = 'Val-Cenis'`                                           |

---

### 2) BAN – Base Adresse Nationale (IGN)
|        |                                                                 |
|-----------------|------------------------------------------------------------------------|
| **Nom du flux** | BAN PLUS adresse                                           |
| **Fournisseur**    | IGN                                                                    |
| **Accès (WFS)**    | https://data.geopf.fr/annexes/ressources/wfs/adresse.xml               |
| **Filtre appliqué**| `nom_commune = 'Val-Cenis'`                                            |

---

### 3) Bâtiments – BD TOPO (IGN)
|        |                                                                 |
|-----------------|------------------------------------------------------------------------|
| **Nom du dossier**  | BD TOPO – Département D073 (Savoie)                                    |
| **Fournisseur**     | IGN                                                                    |
| **Format source**   | GPKG                                                                   |
| **Adresse de téléchargement** | https://geoservices.ign.fr/bdtopo                                      |
| **Couche utilisée** | `batiment` (détection automatique)                                     |
| **Traitement** | Le script récupère le lien `.7z`de BD TOPO – Département D073 le plus récent, télécharge, extrait, découpe sur l’emprise Val-Cenis |

---

### 4) Sommets – Geofabrik Rhône-Alpes (OSM)

|        |                                                                 |
|-----------------|------------------------------------------------------------------------|
| **Nom du dossier**   | Données OSM – Geofabrik Rhône-Alpes                                    |
| **Fournisseur**     | OpenStreetMap / Geofabrik                                              |
| **Format source**   | SHP                                                                   |
|  **Adresse de téléchargement**  | https://download.geofabrik.de/europe/france/rhone-alpes-latest-free.shp.zip |
| **Couche utilisée** | `gis_osm_natural_free_1`                                               |
| **Filtre appliqué** | `"peak"` (sur champ `fclass`)                                          |

## Structure

**Base de données** : `etl_vc`  
**Schéma** : `vc_etl`

---

### Table : `val_cenis`

| Champs            | Types         | Description   |
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
| Identiques au flux  | Identiques au flux  | Champs et types récupérés du flux                |

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
 3. Modifier les fichiers .bat avec vos chemins personnels (détaillé dans les fichiers .bat)
 4. Modifier le mot de passe de votre base de données dans le fichier "config.py"
 5. Exécuter le fichier .bat "run_pipeline_full.bat" (il faut changer les chemins à l'intérieur au préalable), ou dans un terminal exploitant l'environnement python la commande "python pipeline.py --full" (uniquement lorsque vous êtes placé dans le dossier contenant les scripts);
 6. Le script va exécuter le processus ETL automatiquement jusqu'à sa complétion ;
 7. Une fois terminé, la base de données "etl_vc" sera alimentée, les données seront stockées dans le schéma "vc_etl" ;
 8. Pour mettre à jour les données, lancer le fichier .bat "run_pipeline_upadate.bat", ou dans un terminal exploitant l'environnement python la commande "python pipeline.py --update". La mise à jour ne s'effectuera que si le "run_pipeline_full.bat" a été exécuté il y a plus de trente jours. Si vous souhaitez mettre à jour avant les trente jours, il faut exécuter le "run_pipeline_full.bat" ou la commande "python pipeline.py --full" à nouveau.
