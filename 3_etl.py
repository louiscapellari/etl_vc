import sys
import logging
import warnings
from datetime import datetime

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from sqlalchemy import create_engine, text

from shapely.geometry import MultiPolygon
try:
    from shapely.validation import make_valid as shapely_make_valid
except Exception:
    shapely_make_valid = None

# config projet
from config import (
    LOGGING, DOSSIER_DONNEES, DOSSIER_SORTIE, CRS_CIBLE,
    DATABASE, VAL_CENIS, SOMMETS
)

warnings.filterwarnings("ignore", category=UserWarning)

logging.basicConfig(
    level=getattr(logging, LOGGING["level"]),
    format=LOGGING["format"],
    handlers=[logging.FileHandler(str(LOGGING["file"])),
              logging.StreamHandler(sys.stdout)],
)
logs = logging.getLogger("etl")


def activ_postgis(conn_or_engine):
    try:
        # psycopg2 connection
        if hasattr(conn_or_engine, "cursor"):
            with conn_or_engine.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            conn_or_engine.commit()
            return
        # SQLAlchemy Engine
        connect = getattr(conn_or_engine, "connect", None)
        if connect is not None:
            with connect() as con:
                try:
                    con.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")
                except Exception:
                    con.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            return
        # SQLAlchemy Connection
        exec_sql = getattr(conn_or_engine, "exec_driver_sql", None)
        if exec_sql is not None:
            conn_or_engine.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")
            return
    except Exception as e:
        try:
            logs.warning(f"Impossible d'activer PostGIS automatiquement : {e}")
        except Exception:
            pass


def conn_pg():
    # ouvre une connexion
    conn = psycopg2.connect(
        host=DATABASE["host"],
        port=DATABASE["port"],
        dbname=DATABASE["database"],
        user=DATABASE["user"],
        password=DATABASE["password"],
    )
    try:
        activ_postgis(conn)
    except Exception:
        pass
    return conn

def forcer_2154(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Reprojette en 2154 si nécessaire
    if gdf is None or gdf.empty:
        return gdf
    if str(gdf.crs) != CRS_CIBLE:
        return gdf.to_crs(CRS_CIBLE)
    return gdf

def truncate_r(moteur, schema, table):
    # Truncate
    with moteur.begin() as con:
        con.execute(text(
            f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY CASCADE;'
        ))

def ajouter_col_geom(schema, table, con, cur, typemod_geom):
    # ajoute la colonne geom si absente + index
    cur.execute("""
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema=%s AND table_name=%s AND column_name='geom'
    """, [schema, table])
    if not cur.fetchone():
        cur.execute(
            sql.SQL('ALTER TABLE {}.{} ADD COLUMN geom {}')
               .format(sql.Identifier(schema),
                       sql.Identifier(table),
                       sql.SQL(typemod_geom))
        )
        logs.info(f"Colonne geom ajoutée sur {schema}.{table} ({typemod_geom}).")
    
    cur.execute(
        sql.SQL("""CREATE INDEX IF NOT EXISTS {} ON {}.{} USING GIST(geom)""")
           .format(sql.Identifier(f"idx_{table}_geom"),
                   sql.Identifier(schema),
                   sql.Identifier(table))
    )
    con.commit()

def geom_valide(g):
    # géométrie utilisable
    if g is None or g.is_empty:
        return None
    try:
        gg = shapely_make_valid(g) if shapely_make_valid else g
        if gg.geom_type == "Polygon":
            gg = MultiPolygon([gg])
        return gg
    except Exception:
        try:
            gg = g.buffer(0)
            if gg.is_empty:
                return None
            if gg.geom_type == "Polygon":
                return MultiPolygon([gg])
            return gg
        except Exception:
            return None

def maj_geom_massive(con, schema: str, table: str, wkbs, cible: str):
    # update de la colonne geom
    with con.cursor() as cur:
        # récupère l'ordre des fid
        cur.execute(sql.SQL('SELECT fid FROM {}.{} ORDER BY fid')
                    .format(sql.Identifier(schema), sql.Identifier(table)))
        fids = [r[0] for r in cur.fetchall()]
        lignes = [(fid, psycopg2.Binary(wkb) if wkb is not None else None)
                  for fid, wkb in zip(fids, wkbs)]

        # temp table + insert
        cur.execute("DROP TABLE IF EXISTS tmp_geom;")
        cur.execute("CREATE TEMP TABLE tmp_geom(fid BIGINT, wkb BYTEA);")
        extras.execute_values(cur, "INSERT INTO tmp_geom(fid, wkb) VALUES %s",
                              lignes, page_size=5000)

        # expression SQL selon le type visé
        if cible == "POINT":
            expr = "ST_Force2D(ST_SetSRID(ST_GeomFromWKB(t.wkb),2154))"
        else:
            expr = ("ST_Multi(ST_CollectionExtract("
                    "ST_Force2D(ST_SetSRID(ST_GeomFromWKB(t.wkb),2154)),3))" )

        # update
        cur.execute(sql.SQL(f"""
            UPDATE {schema}.{table} AS s
               SET geom = {expr}
              FROM tmp_geom t
             WHERE s.fid = t.fid AND t.wkb IS NOT NULL;
        """))
        con.commit()


# classe principale
class TransformateurDonnees:
    
    def __init__(self):
        self.schema = "vc_etl"    # schéma cible
        # moteur SQLAlchemy
        self.moteur = create_engine(
            f"postgresql://{DATABASE['user']}:{DATABASE['password']}"
            f"@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['database']}"
        )
        # Activation PostGIS
        try:
            activ_postgis(self.moteur)
        except Exception:
            pass

        self.dossier_data = DOSSIER_DONNEES
        self.dossier_sortie = DOSSIER_SORTIE

    # VAL-CENIS
    def traiter_val_cenis(self):
        logs.info("Traitement Val-Cenis.")
        try:
            src = self.dossier_data / "val_cenis_raw.gpkg"
            if not src.exists():
                logs.error(f"Erreur : fichier manquant {src}")
                return False

            # mapping champs et nettoyage
            gdf = forcer_2154(gpd.read_file(src))
            mp = VAL_CENIS["fields_mapping"]
            df = pd.DataFrame({dst: (gdf[src] if src in gdf.columns else None)
                               for dst, src in mp.items()
                               if dst != "fid" and src is not None})
            df["geometry"] = gdf.geometry
            gdf_out = gpd.GeoDataFrame(df, crs=gdf.crs)

            for c in ("population", "supf_cadas"):
                if c in gdf_out:
                    gdf_out[c] = pd.to_numeric(gdf_out[c], errors="coerce").fillna(0).astype(int)
            if "date_recensement" in gdf_out:
                gdf_out["date_recensement"] = pd.to_datetime(gdf_out["date_recensement"], errors="coerce")

            # export GPKG
            gdf_out.to_file(self.dossier_sortie / "val_cenis.gpkg", driver="GPKG")

            # chargement attributaire
            truncate_r(self.moteur, self.schema, "val_cenis")
            gdf_out.drop(columns=["geometry"]).to_sql(
                "val_cenis", self.moteur, schema=self.schema,
                if_exists="append", index=False, method="multi"
            )

            # mise à jour géométrie
            con = conn_pg()
            with con.cursor() as cur:
                ajouter_col_geom(self.schema, "val_cenis", con, cur, "geometry(MULTIPOLYGON,2154)")
            wkbs = [geom_valide(geom).wkb
                    if geom_valide(geom) is not None else None
                    for geom in gdf_out.geometry]
            maj_geom_massive(con, self.schema, "val_cenis", wkbs, cible="MULTIPOLYGON")
            con.close()

            logs.info(f"Val-Cenis chargé ({len(gdf_out)} lignes).")
            return True
        except Exception as e:
            logs.error(f"Erreur : Val-Cenis — {e}")
            return False

    # BAN
    def rech_type_pg(self, s: pd.Series) -> str:
        # déduction de type PostgreSQL
        d = str(s.dtype)
        if d.startswith("int"): return "INTEGER"
        if d.startswith("float"): return "DOUBLE PRECISION"
        if d.startswith("bool"): return "BOOLEAN"
        if d.startswith("datetime"): return "TIMESTAMP"
        return "TEXT"

    def traiter_ban(self):
        logs.info("Traitement BAN.")
        try:
            src = self.dossier_data / "ban_raw.gpkg"
            if not src.exists():
                logs.info("BAN absente, étape ignorée.")
                return False

            gdf = forcer_2154(gpd.read_file(src))

            # table créée dynamiquement selon colonnes détectées
            con = conn_pg(); cur = con.cursor()
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.ban CASCADE;')
                        .format(sql.Identifier(self.schema)))
            colonnes = ['fid BIGSERIAL PRIMARY KEY']
            for c in gdf.columns:
                if c == "geometry":
                    continue
                pg = self.rech_type_pg(gdf[c])
                propre = (c.replace(" ", "_").replace("-", "_")
                           .replace("(", "").replace(")", "").lower())
                colonnes.append(f'"{propre}" {pg}')
            cur.execute(f'CREATE TABLE "{self.schema}"."ban" ({", ".join(colonnes)});')
            con.commit()

            # insert attributaire
            df_attr = gdf.drop(columns=["geometry"]).copy()
            df_attr.columns = [(c.replace(" ", "_").replace("-", "_")
                                 .replace("(", "").replace(")", "").lower())
                                for c in df_attr.columns]
            df_attr.to_sql("ban", self.moteur, schema=self.schema,
                           if_exists="append", index=False, method="multi")

            # col geom + update
            ajouter_col_geom(self.schema, "ban", con, cur, "geometry(POINT,2154)")
            wkbs = [geom.wkb if geom is not None else None for geom in gdf.geometry]
            maj_geom_massive(con, self.schema, "ban", wkbs, cible="POINT")
            con.close()

            # export GPKG
            gdf.to_file(self.dossier_sortie / "ban.gpkg", driver="GPKG")
            logs.info(f"BAN chargée ({len(gdf)} lignes).")
            return True
        except Exception as e:
            logs.error(f"Erreur : BAN — {e}")
            return False

    # SOMMETS
    def traiter_sommets(self):
        logs.info("Traitement Sommets.")
        try:
            src = self.dossier_data / "sommets_raw.gpkg"
            if not src.exists():
                logs.info("Sommets absents, étape ignorée.")
                return False

            # mapping champs
            gdf = forcer_2154(gpd.read_file(src))
            mp = SOMMETS["fields_mapping"]
            df = pd.DataFrame({dst: (gdf[src] if src in gdf.columns else None)
                               for dst, src in mp.items() if dst != "fid"})
            df["altitude"] = pd.to_numeric(df.get("elevation", None), errors="coerce")
            if "nom" not in df and "name" in gdf.columns:
                df["nom"] = gdf["name"]
            df["geometry"] = gdf.geometry
            gdf_out = gpd.GeoDataFrame(df[["osm_id", "nom", "altitude", "geometry"]].copy(),
                                       crs=gdf.crs)

            # export + chargement attributaire
            gdf_out.to_file(self.dossier_sortie / "sommets.gpkg", driver="GPKG")
            truncate_r(self.moteur, self.schema, "sommets")
            gdf_out.drop(columns=["geometry"]).to_sql(
                "sommets", self.moteur, schema=self.schema,
                if_exists="append", index=False, method="multi"
            )

            # géométrie POINT
            con = conn_pg(); cur = con.cursor()
            ajouter_col_geom(self.schema, "sommets", con, cur, "geometry(POINT,2154)")
            wkbs = [geom.wkb if geom is not None else None for geom in gdf_out.geometry]
            maj_geom_massive(con, self.schema, "sommets", wkbs, cible="POINT")
            con.close()

            logs.info(f"Sommets chargés ({len(gdf_out)} lignes).")
            return True
        except Exception as e:
            logs.error(f"Erreur : Sommets — {e}")
            return False

    # BÂTIMENTS
    def traiter_batiments(self):
        logs.info("Traitement Bâtiments.")
        try:
            src = self.dossier_data / "batiments_raw.gpkg"
            if not src.exists():
                logs.info("Bâtiments absents, étape ignorée.")
                return False

            gdf = gpd.read_file(src)
            if gdf.empty:
                logs.info("Bâtiments vides.")
                return False
            gdf = forcer_2154(gdf)

            # structure cible
            con = conn_pg(); cur = con.cursor()
            cur.execute(sql.SQL('DROP TABLE IF EXISTS {}.batiments CASCADE;')
                        .format(sql.Identifier(self.schema)))
            cur.execute(sql.SQL(f"""
                CREATE TABLE {self.schema}.batiments (
                    fid BIGSERIAL PRIMARY KEY,
                    cleabs TEXT, nature TEXT, usage1 TEXT, usage2 TEXT,
                    construc_legere BOOLEAN, etat_obj TEXT,
                    date_crea TIMESTAMP, date_modif TIMESTAMP,
                    date_apparition DATE, date_confirm DATE,
                    sources TEXT, id_sources TEXT,
                    methode_acquis_plani TEXT, methode_acquis_alti TEXT,
                    precision_plani REAL, precision_alti REAL,
                    nombre_logements INTEGER, nombre_etages INTEGER,
                    materiaux_murs TEXT, materiaux_toiture TEXT,
                    hauteur DOUBLE PRECISION,
                    alti_mini_sol DOUBLE PRECISION, alti_mini_toit DOUBLE PRECISION,
                    alti_max_toit DOUBLE PRECISION, alti_max_sol DOUBLE PRECISION,
                    origine_bat TEXT, appariement_fonciers TEXT, id_rnb TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            con.commit()

            # mapping champs
            mp = {
                'cleabs':'cleabs','nature':'nature',
                'usage1':'usage_1','usage2':'usage_2',
                'construc_legere':'construction_legere',
                'etat_obj':'etat_de_l_objet',
                'date_crea':'date_creation','date_modif':'date_modification',
                'date_apparition':'date_d_apparition','date_confirm':'date_de_confirmation',
                'sources':'sources','id_sources':'identifiants_sources',
                'methode_acquis_plani':'methode_d_acquisition_planimetrique',
                'methode_acquis_alti':'methode_d_acquisition_altimetrique',
                'precision_plani':'precision_planimetrique','precision_alti':'precision_altimetrique',
                'nombre_logements':'nombre_de_logements','nombre_etages':'nombre_d_etages',
                'materiaux_murs':'materiaux_des_murs','materiaux_toiture':'materiaux_de_la_toiture',
                'hauteur':'hauteur',
                'alti_mini_sol':'altitude_minimale_sol','alti_mini_toit':'altitude_minimale_toit',
                'alti_max_toit':'altitude_maximale_toit','alti_max_sol':'altitude_maximale_sol',
                'origine_bat':'origine_du_batiment',
                'appariement_fonciers':'appariement_fichiers_fonciers',
                'id_rnb':'identifiants_rnb'
            }
            df = pd.DataFrame({dst: (gdf[src] if src in gdf.columns else None)
                               for dst, src in mp.items()})

            # normalisation simple
            if 'construc_legere' in df:
                df['construc_legere'] = df['construc_legere'].astype(str).str.lower().map(
                    {'true': True, '1': True, 'oui': True, 'false': False, '0': False, 'non': False}
                )
            for c in ('nombre_logements', 'nombre_etages'):
                if c in df: df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
            for c in ('precision_plani','precision_alti','hauteur',
                      'alti_mini_sol','alti_mini_toit','alti_max_toit','alti_max_sol'):
                if c in df: df[c] = pd.to_numeric(df[c], errors='coerce')
            for c in ('date_crea','date_modif'):
                if c in df: df[c] = pd.to_datetime(df[c], errors='coerce')
            for c in ('date_apparition','date_confirm'):
                if c in df: df[c] = pd.to_datetime(df[c], errors='coerce').dt.date

            # insertion attributaire
            df.to_sql('batiments', self.moteur, schema=self.schema,
                      if_exists='append', index=False, method='multi', chunksize=2000)

            # col geom + update
            ajouter_col_geom(self.schema, 'batiments', con, cur, "geometry(MULTIPOLYGON,2154)")
            wkbs, invalides = [], 0
            for geom in gdf.geometry:
                vg = geom_valide(geom)
                wkbs.append(vg.wkb if vg is not None else None)
                if vg is None:
                    invalides += 1
            maj_geom_massive(con, self.schema, "batiments", wkbs, cible="MULTIPOLYGON")

            cur.execute(sql.SQL("""
                SELECT COUNT(*) FILTER (WHERE geom IS NOT NULL), COUNT(*)
                  FROM {}.batiments
            """).format(sql.Identifier(self.schema)))
            avec_geom, total = cur.fetchone()
            con.close()

            # export GPKG
            gdf_clean = gdf.copy()
            gdf_clean["geometry"] = [geom_valide(geom) for geom in gdf.geometry]
            gdf_clean = gdf_clean.set_crs(epsg=2154, allow_override=True)
            gdf_clean.to_file(self.dossier_sortie / "batiments.gpkg", driver="GPKG")

            logs.info(f"Bâtiments chargés : {avec_geom} / {total} géométries valides (sources invalides : {invalides}).")
            return avec_geom > 0
        except Exception as e:
            logs.error(f"Erreur : Bâtiments — {e}")
            return False

    # Finalisation
    def finaliser(self):
        try:
            from datetime import timezone
            ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
            con = conn_pg(); cur = con.cursor()

            # index attributaires
            requetes_idx = [
                f'CREATE INDEX IF NOT EXISTS idx_val_cenis_insee ON "{self.schema}"."val_cenis"(insee_code);',
                f'CREATE INDEX IF NOT EXISTS idx_val_cenis_nom   ON "{self.schema}"."val_cenis"(nom);',
                f'CREATE INDEX IF NOT EXISTS idx_sommets_nom     ON "{self.schema}"."sommets"(nom) WHERE nom IS NOT NULL;',
                f'CREATE INDEX IF NOT EXISTS idx_sommets_altitude ON "{self.schema}"."sommets"(altitude);',
                f'CREATE INDEX IF NOT EXISTS idx_batiments_cleabs ON "{self.schema}"."batiments"(cleabs);',
                f'CREATE INDEX IF NOT EXISTS idx_batiments_nature ON "{self.schema}"."batiments"(nature);',
                f'CREATE INDEX IF NOT EXISTS idx_batiments_usage1 ON "{self.schema}"."batiments"(usage1);',
            ]
            for q in requetes_idx:
                try:
                    cur.execute(q)
                except Exception as e:
                    logs.info(f"Index ignoré : {e}")

            # colonne "updated_at" si absente
            for t in ("val_cenis","ban","sommets","batiments"):
                cur.execute("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s AND column_name='updated_at'
                """, [self.schema, t])
                if not cur.fetchone():
                    cur.execute(sql.SQL(
                        'ALTER TABLE {}.{} ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;'
                    ).format(sql.Identifier(self.schema), sql.Identifier(t)))

            # commentaires tables bdd
            commentaires = {
                "val_cenis":  f"Depuis flux WFS admin express IGN - Dernière mise à jour : {ts}",
                "ban":        f"Depuis flux WFS BAN IGN - Dernière mise à jour : {ts}",
                "sommets":    f"Depuis gis_osm_natural_free_1 OSM - Dernière mise à jour : {ts}",
                "batiments":  f"Depuis BD TOPO IGN - Dernière mise à jour : {ts}",
            }
            for t, c in commentaires.items():
                cur.execute(sql.SQL("COMMENT ON TABLE {}.{} IS %s;")
                            .format(sql.Identifier(self.schema), sql.Identifier(t)), [c])

            # stats
            for t in ("val_cenis","ban","sommets","batiments"):
                try:
                    cur.execute(sql.SQL('SELECT COUNT(*), COUNT(geom) FROM {}.{}')
                                .format(sql.Identifier(self.schema), sql.Identifier(t)))
                    tot, wgeom = cur.fetchone()
                    logs.info(f"{t} : {tot} enregistrements ({wgeom} avec géométrie).")
                except Exception as e:
                    logs.info(f"Statistiques ignorées pour {t} : {e}")

            con.commit(); cur.close(); con.close()
            logs.info("Finalisation terminée.")
            return True
        except Exception as e:
            logs.error(f"Erreur : Finalisation — {e}")
            return False

    # Orchestrateur
    def executer(self):
        # enchaînement des blocs, arrêt si Val-Cenis absent
        res = {}
        logs.info("Démarrage ETL.")
        res["val_cenis"] = self.traiter_val_cenis()
        if not res["val_cenis"]:
            logs.error("Erreur : Val-Cenis indispensable, arrêt.")
            return res
        res["ban"] = self.traiter_ban()
        res["sommets"] = self.traiter_sommets()
        res["batiments"] = self.traiter_batiments()
        res["finalize"] = self.finaliser()
        return res


# main
def main():
    try:
        tr = TransformateurDonnees()
        # test connexion SQLAlchemy
        with tr.moteur.connect() as c:
            c.execute(text("SELECT 1"))
        resultats = tr.executer()
        ok = sum(1 for v in resultats.values() if v)
        logs.info(f"Bilan : {ok}/{len(resultats)} étapes réussies.")
        return bool(resultats.get("val_cenis", False) and resultats.get("batiments", False))
    except Exception as e:
        logs.error(f"Erreur : ETL — {e}")
        return False
    finally:
        logs.info(f"Fin ETL - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
