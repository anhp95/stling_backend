"""
Catalog Service — pure domain logic for querying the internal CLDF database.
"""

import math
import os
from typing import List, Dict, Any, Optional
from app.db import get_db_connection

DATA_ROOT = "data"
GLOSS_INDEX_PARQUET = os.path.join(
    DATA_ROOT, "parquet", "concepticon_gloss_index.parquet"
).replace("\\", "/")
DISTINCT_GLOSS_PARQUET = os.path.join(
    DATA_ROOT, "parquet", "distinct_concepticon_glosses.parquet"
).replace("\\", "/")


def get_coordinate_filter_sql(table_alias=""):
    prefix = f"{table_alias}." if table_alias else ""
    return f"({prefix}Latitude IS NOT NULL AND {prefix}Longitude IS NOT NULL AND TRY_CAST({prefix}Latitude AS DOUBLE) != 0.0 AND TRY_CAST({prefix}Longitude AS DOUBLE) != 0.0)"


def fetch_internal_data_csv(
    glosses: List[str],
    dataset: str = "Combined",
    data_type: str = "spoken_language",
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_km: Optional[float] = None,
) -> str:
    """
    Fetch linguistic data from the internal CLDF database and return as CSV string.
    """
    con = get_db_connection()
    try:
        if dataset == "Combined":
            if not glosses:
                return "No glosses specified."

            spoken_dir = os.path.join(DATA_ROOT, "parquet", "spoken_language")
            if not os.path.exists(spoken_dir):
                return "Internal data directory not found."

            all_datasets = [
                d
                for d in os.listdir(spoken_dir)
                if os.path.isdir(os.path.join(spoken_dir, d))
            ]

            gloss_list = "', '".join([g.replace("'", "''") for g in glosses])

            # Find which datasets actually have these glosses
            try:
                matching_datasets = (
                    con.execute(
                        f"SELECT DISTINCT dataset_name FROM read_parquet('{GLOSS_INDEX_PARQUET}') WHERE Concepticon_Gloss IN ('{gloss_list}')"
                    )
                    .df()["dataset_name"]
                    .tolist()
                )
                datasets = [d for d in all_datasets if d in set(matching_datasets)]
            except:
                datasets = all_datasets

            spatial_sql = ""
            if lat is not None and lon is not None and radius_km is not None:
                lat_diff = radius_km / 111.0
                lon_diff = (
                    radius_km / (111.0 * math.cos(math.radians(lat)))
                    if math.cos(math.radians(lat)) != 0
                    else lat_diff
                )
                spatial_sql = f" AND TRY_CAST(l.Latitude AS DOUBLE) BETWEEN {lat - lat_diff} AND {lat + lat_diff} AND TRY_CAST(l.Longitude AS DOUBLE) BETWEEN {lon - lon_diff} AND {lon + lon_diff}"

            subqueries = []
            for d in datasets:
                ds_path = os.path.join(spoken_dir, d)
                l_p = os.path.join(ds_path, "languages.parquet").replace("\\", "/")
                f_p = os.path.join(ds_path, "forms.parquet").replace("\\", "/")
                p_p = os.path.join(ds_path, "parameters.parquet").replace("\\", "/")
                if all(os.path.exists(p) for p in [l_p, f_p, p_p]):
                    subqueries.append(
                        f"""
                        SELECT l.ID, l.Name, l.Glottocode, CAST(l.Latitude AS DOUBLE) as Latitude, CAST(l.Longitude AS DOUBLE) as Longitude, f.Value as form_value, p.Concepticon_Gloss as parameter_name, '{d}' as dataset_name
                        FROM read_parquet('{p_p}') p
                        JOIN read_parquet('{f_p}') f ON p.ID = f.Parameter_ID
                        JOIN read_parquet('{l_p}') l ON f.Language_ID = l.ID
                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {get_coordinate_filter_sql('l')}{spatial_sql}
                    """
                    )

            if not subqueries:
                return "No matching data found in any internal dataset."

            source_query = "(" + " UNION ALL ".join(subqueries) + ")"
        else:
            dataset_path = os.path.join(DATA_ROOT, "parquet", data_type, dataset)
            if not os.path.exists(dataset_path):
                return f"Dataset '{dataset}' not found."

            l_p = os.path.join(dataset_path, "languages.parquet").replace("\\", "/")
            f_p = os.path.join(dataset_path, "forms.parquet").replace("\\", "/")
            p_p = os.path.join(dataset_path, "parameters.parquet").replace("\\", "/")

            spatial_sql = ""
            if lat is not None and lon is not None and radius_km is not None:
                lat_diff = radius_km / 111.0
                lon_diff = (
                    radius_km / (111.0 * math.cos(math.radians(lat)))
                    if math.cos(math.radians(lat)) != 0
                    else lat_diff
                )
                spatial_sql = f" AND TRY_CAST(l.Latitude AS DOUBLE) BETWEEN {lat - lat_diff} AND {lat + lat_diff} AND TRY_CAST(l.Longitude AS DOUBLE) BETWEEN {lon - lon_diff} AND {lon + lon_diff}"

            gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
            source_query = f"""
                (
                    SELECT l.ID, l.Name, l.Glottocode, CAST(l.Latitude AS DOUBLE) as Latitude, CAST(l.Longitude AS DOUBLE) as Longitude, f.Value as form_value, p.Concepticon_Gloss as parameter_name
                    FROM read_parquet('{p_p}') p
                    JOIN read_parquet('{f_p}') f ON p.ID = f.Parameter_ID
                    JOIN read_parquet('{l_p}') l ON f.Language_ID = l.ID
                    WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {get_coordinate_filter_sql('l')}{spatial_sql}
                )
            """

        query = f"SELECT * FROM {source_query}"
        df = con.execute(query).df()

        if df.empty:
            return "No data found for the specified concepts."

        import io

        output = io.StringIO()
        df.to_csv(output, index=False)
        return output.getvalue()

    except Exception as e:
        return f"Error fetching internal data: {str(e)}"
    finally:
        con.close()


def search_glosses(query: str = "") -> List[str]:
    """Search for available concepts in the internal catalog."""
    if not os.path.exists(DISTINCT_GLOSS_PARQUET):
        return []

    con = get_db_connection()
    try:
        sql = f"SELECT Concepticon_Gloss FROM read_parquet('{DISTINCT_GLOSS_PARQUET}')"
        if query:
            clean_q = query.replace("'", "''").lower()
            sql += f" WHERE LOWER(Concepticon_Gloss) LIKE '%{clean_q}%'"
        sql += f" ORDER BY Concepticon_Gloss"

        df = con.execute(sql).df()
        return df["Concepticon_Gloss"].tolist()
    except:
        return []
    finally:
        con.close()
