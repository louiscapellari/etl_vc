# Pipeline ETL données géospatiales sur la commune de Val-Cenis 

Pipeline ETL reproductible sur la commune de **Val-Cenis** (Savoie, 73, France) qui : extrait (via WFS et téléchargements OSM et BDTOPO), transforme (nettoyage, formatage, mapping, découpage), charge dans une base de données PostgreSQL/PostGIS quatre couches, avec une possibilité de maintenance via mise à jour. 


## Objectifs
- Automatiser l’acquisition de données géospatiales pour la commune de Val-Cenis (via utilisation de WFS / téléchargements) ;
- Filtrer/découper par l’emprise de Val-Cenis ;
- Créer les tables PostgreSQL/PostGIS au bon format et avec les types corrects ;
- Charger automatiquement les attributs et les géométries, indexer, mettre un timestamp, mettre des commentaires ;
- Planifier une mise à jour mensuelle.

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
| **Couche utilisée** | `batiment`                                     |

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
| gml_id           | text         | -                    |
| cleabs           | text         | -                   |
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
| cleabs                 | text         | - |
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
Testé dans un environnement python 3.12.11 généré via Anaconda.<br>
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
 1. Créez un environnement python disposant de toutes les librairies mentionnées ci-dessus ;
 2. Créez une base de données PostgreSQL/PostGIS (l'ajout de l'extension PostGIS est prévue dans le script, mais vous pouvez aussi le faire au moment de la création de la base de données) nommée `etl_vc` ;
 3. Modifiez le mot de passe, et éventuellement le nom d'utilisateur de votre base de données dans le fichier `config.py` ;
 4. Exécutez le fichier `pipeline.py` dans un terminal exploitant l'environnement python la commande `python pipeline.py --full`, uniquement lorsque vous êtes placé dans le dossier contenant les scripts, (exemple dans le terminal : `cd "chemin du dossier contenant les scripts"`). ;
 5. Le script va exécuter le processus ETL automatiquement jusqu'à sa complétion ;
 6. Les données téléchargées seront stockées dans un dossier temporaire "vc-tmp" dans les "Documents" sous Windows ou dans le sous-dossier "temp" au sein du dossier contenant les scripts si vous utilisez un autre système d'exploitation.
 7. Une fois terminé, la base de données `etl_vc` sera alimentée, les données seront stockées dans le schéma `vc_etl` ;
 8. Pour mettre à jour les données, exécutez dans un terminal exploitant l'environnement python la commande `python pipeline.py --update` uniquement lorsque vous êtes placé dans le dossier contenant les scripts, (exemple dans le terminal : `cd "chemin du dossier contenant les scripts"`). La mise à jour ne s'effectuera que si le pipeline a été exécuté il y a plus de trente jours. Si vous souhaitez mettre à jour avant les trente jours, il faut exécuter la commande `python pipeline.py --full` à nouveau.
 9. Il est possible de créer un fichier .bat qui active l'environnement python et qui se place dans le dossier contenant les scripts pour une exécution totalement automatisée.

## Résultats 
Lorsque le processus est correctement achevé, vous devriez obtenir ce type de résultat : <br> 
Console :<br> 
<img width="432" height="44" alt="etl_fin" src="https://github.com/user-attachments/assets/7f177e38-b03d-41d1-95c3-750c89cd746d" /> <br> 
PG Admin :<br> 
<img width="382" height="425" alt="etl_postgres" src="https://github.com/user-attachments/assets/6d26f4d2-1188-4b9c-850e-d8d1ae11f47e" /> <br> 
QGIS :<br> 
<img width="963" height="503" alt="etl_qgis" src="https://github.com/user-attachments/assets/e9c44176-fd6d-4fbe-9184-2b53ad0da9eb" /> <br> 


