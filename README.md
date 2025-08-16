# ETL données géospatiales Val-Cenis 

ETL reproductible pour Val-Cenis (Savoie, 73, France) qui : extrait (WFS/OSM/BDTOPO), transforme (nettoyage + mapping), charge dans une base de donnée PostgreSQL/PostGIS intitulée "etl_vc" et s'exécute via pipeline.py ou run_pipeline_full.bat. Avec la possibilité d'une mise à jour mensuelle via la commande "python pipeline.py --update" dans un environnement python ou utiliser le run_pipeline_update.bat. 

## Objectifs
- Automatiser l’acquisition de données géospatiales (WFS / téléchargement) ;
- Filtrer/découper par l’emprise de **Val-Cenis** ;
- Créer les tables **PostgreSQL/PostGIS** au bon format ;
- Charger les attributs + géométries, indexer, timestamp, commentaires ;
- Planifier une **mise à jour mensuelle** (état + backup).


