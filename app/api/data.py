from fastapi import APIRouter, HTTPException, Query, Response
from app.db import get_db_connection
import os
import io
import pyarrow as pa
import json
from typing import Optional, List, Any
import numpy as np

router = APIRouter()

# Data Root is now Parquet
DATA_ROOT = "data"
GLOSS_INDEX_PARQUET = os.path.join(
    DATA_ROOT, "parquet", "concepticon_gloss_index.parquet"
).replace("\\", "/")
DISTINCT_GLOSS_PARQUET = os.path.join(
    DATA_ROOT, "parquet", "distinct_concepticon_glosses.parquet"
).replace("\\", "/")

# Common coordinate column pairs to check
COORD_PAIRS = [
    ("Latitude", "Longitude"),
    ("latitude", "longitude"),
    ("Lat", "Lon"),
    ("lat", "lon"),
    ("lat", "lng"),
    ("y", "x"),
]


def get_coordinate_filter_sql(table_alias=""):
    """
    Generates a SQL fragment to filter out rows without coordinate information.
    Checks common coordinate column names.
    Uses TRY_CAST to DOUBLE to avoid errors with string coordinates and compares against 0.0.
    """
    prefix = f"{table_alias}." if table_alias else ""
    # Use TRY_CAST to DOUBLE to handle string coords and floating point comparison
    return f"({prefix}Latitude IS NOT NULL AND {prefix}Longitude IS NOT NULL AND TRY_CAST({prefix}Latitude AS DOUBLE) != 0.0 AND TRY_CAST({prefix}Longitude AS DOUBLE) != 0.0)"


def sanitize_df(df):
    """
    Robustly convert NaN/Inf/NaT to None for JSON compliance.
    """
    return [
        {
            k: (
                v
                if v is not np.nan
                and v == v
                and not (isinstance(v, float) and np.isinf(v))
                else None
            )
            for k, v in row.items()
        }
        for row in df.to_dict(orient="records")
    ]


COMBINED_DATASET_NAME = "Combined"


@router.get("/catalog")
async def get_catalog(glosses: Optional[List[str]] = Query(None)):
    catalog = {
        "spoken_language": [],
        "sign_language": [],
        "archaeology": [],
        "genetics": [],
    }
    con = get_db_connection()
    try:
        matching_set = None
        gloss_list = ""
        if glosses:
            gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
            try:
                matching_datasets = (
                    con.execute(
                        f"""
                    SELECT DISTINCT dataset_name 
                    FROM read_parquet('{GLOSS_INDEX_PARQUET}')
                    WHERE Concepticon_Gloss IN ('{gloss_list}')
                 """
                    )
                    .df()["dataset_name"]
                    .tolist()
                )
                matching_set = set(matching_datasets)
            except Exception:
                # Fallback or error handling if index doesn't exist yet
                pass

        combined_spoken_langs = 0

        for data_type in catalog.keys():
            type_dir_check = os.path.join(DATA_ROOT, "parquet", data_type)

            if os.path.exists(type_dir_check):
                dirs = [
                    d
                    for d in os.listdir(type_dir_check)
                    if os.path.isdir(os.path.join(type_dir_check, d))
                ]
                for d in dirs:
                    if data_type == "spoken_language" and matching_set is not None:
                        if d not in matching_set:
                            continue

                    dataset_path = os.path.join(type_dir_check, d)
                    count = 0
                    try:
                        if data_type in ["spoken_language", "sign_language"]:
                            # Look for languages.parquet
                            lang_parquet = os.path.join(
                                dataset_path, "languages.parquet"
                            ).replace("\\", "/")
                            if os.path.exists(lang_parquet):
                                if data_type == "spoken_language" and glosses:
                                    # Filtered unique languages
                                    forms_parquet = os.path.join(
                                        dataset_path, "forms.parquet"
                                    ).replace("\\", "/")
                                    params_parquet = os.path.join(
                                        dataset_path, "parameters.parquet"
                                    ).replace("\\", "/")

                                    coord_filter = get_coordinate_filter_sql("l")
                                    q = f"""
                                        SELECT count(DISTINCT l.ID)
                                        FROM read_parquet('{lang_parquet}') l
                                        JOIN read_parquet('{forms_parquet}') f ON l.ID = f.Language_ID
                                        JOIN read_parquet('{params_parquet}') p ON f.Parameter_ID = p.ID
                                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {coord_filter}
                                    """
                                    count = con.execute(q).fetchone()[0]
                                else:
                                    # Total languages with coordinates
                                    coord_filter = get_coordinate_filter_sql()
                                    count = con.execute(
                                        f"SELECT count(*) FROM read_parquet('{lang_parquet}') WHERE {coord_filter}"
                                    ).fetchone()[0]
                        else:
                            # Look for <data_type>.parquet
                            parquet_file = os.path.join(
                                dataset_path, f"{data_type}.parquet"
                            ).replace("\\", "/")
                            if os.path.exists(parquet_file):
                                coord_filter = get_coordinate_filter_sql()
                                count = con.execute(
                                    f"SELECT count(*) FROM read_parquet('{parquet_file}') WHERE {coord_filter}"
                                ).fetchone()[0]
                    except Exception:
                        pass

                    if count > 0:
                        catalog[data_type].append({"name": d, "count": count})
                        if data_type == "spoken_language" and glosses:
                            combined_spoken_langs += count

        # Add virtual combined dataset for spoken language
        if glosses and combined_spoken_langs > 0:
            catalog["spoken_language"].insert(
                0, {"name": COMBINED_DATASET_NAME, "count": combined_spoken_langs}
            )

        return catalog
    finally:
        con.close()


@router.get("/glosses")
async def get_glosses(datasets: Optional[List[str]] = Query(None)):
    con = get_db_connection()
    try:
        if datasets:
            ds_list = "', '".join([d.replace("'", "''") for d in datasets])
            query = f"""
                SELECT DISTINCT Concepticon_Gloss 
                FROM read_parquet('{GLOSS_INDEX_PARQUET}')
                WHERE dataset_name IN ('{ds_list}')
                ORDER BY Concepticon_Gloss
            """
        else:
            query = f"""
                SELECT Concepticon_Gloss 
                FROM read_parquet('{DISTINCT_GLOSS_PARQUET}')
                ORDER BY Concepticon_Gloss
            """

        df = con.execute(query).df()
        return {"glosses": df["Concepticon_Gloss"].tolist()}
    except Exception as e:
        # Graceful fallback if index files missing
        print(f"Gloss index error: {e}")
        return {"glosses": []}
    finally:
        con.close()


@router.get("/schema")
async def get_schema(data_type: str, dataset: str):
    if dataset.startswith(COMBINED_DATASET_NAME):
        # Use first available spoken dataset to get schema, and add dataset_name
        spoken_dir = os.path.join(DATA_ROOT, "parquet", "spoken_language")
        if os.path.exists(spoken_dir):
            dirs = [
                d
                for d in os.listdir(spoken_dir)
                if os.path.isdir(os.path.join(spoken_dir, d))
            ]
            if dirs:
                # Get schema from first one, then add dataset_name
                res = await get_schema("spoken_language", dirs[0])
                res["columns"].append(
                    {
                        "name": "dataset_name",
                        "type": "categorical",
                        "raw_type": "VARCHAR",
                    }
                )
                return res
        raise HTTPException(status_code=404, detail="No spoken datasets found")

    # Path inside parquet root
    dataset_path = os.path.join(DATA_ROOT, "parquet", data_type, dataset)
    if not os.path.exists(dataset_path):
        raise HTTPException(status_code=404, detail="Dataset not found")

    con = get_db_connection()
    try:
        source_query = ""
        if data_type in ["spoken_language", "sign_language"]:
            lang_parquet = os.path.join(dataset_path, "languages.parquet").replace(
                "\\", "/"
            )
            forms_parquet = os.path.join(dataset_path, "forms.parquet").replace(
                "\\", "/"
            )
            params_parquet = os.path.join(dataset_path, "parameters.parquet").replace(
                "\\", "/"
            )

            if all(
                os.path.exists(f) for f in [lang_parquet, forms_parquet, params_parquet]
            ):
                source_query = f"""
                (
                    SELECT l.*, f.Value as form_value, p.Concepticon_Gloss as parameter_name
                    FROM read_parquet('{lang_parquet}') l
                    JOIN read_parquet('{forms_parquet}') f ON l.ID = f.Language_ID
                    JOIN read_parquet('{params_parquet}') p ON f.Parameter_ID = p.ID
                    WHERE {get_coordinate_filter_sql('l')}
                    LIMIT 0
                )
                """
            else:
                source_query = f"read_parquet('{lang_parquet}') WHERE {get_coordinate_filter_sql()} LIMIT 0"
        else:
            parquet_file = os.path.join(dataset_path, f"{data_type}.parquet").replace(
                "\\", "/"
            )
            source_query = f"read_parquet('{parquet_file}') WHERE {get_coordinate_filter_sql()} LIMIT 0"

        schema_df = con.execute(f"DESCRIBE SELECT * FROM {source_query}").df()
        columns = []
        for _, row in schema_df.iterrows():
            col_name = str(row["column_name"])
            col_type = str(row["column_type"]).lower()
            if any(
                t in col_type for t in ["int", "float", "double", "decimal", "hugeint"]
            ):
                v_type = "numerical"
            else:
                v_type = "categorical"
            columns.append({"name": col_name, "type": v_type, "raw_type": col_type})
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@router.get("/data")
async def get_data(
    data_type: str,
    dataset: str,
    search: Optional[str] = Query(None),
    glosses: Optional[List[str]] = Query(None),
    form_filter: Optional[str] = Query(None),
    parameter_filter: Optional[str] = Query(None),
    limit: int = Query(100, le=5000),
    offset: int = Query(0, ge=0),
    format: Optional[str] = Query(None),
):
    con = get_db_connection()
    try:
        if dataset.startswith(COMBINED_DATASET_NAME):
            if not glosses:
                if format == "json":
                    return {"data": [], "total": 0}
                else:
                    # Return empty Arrow Response
                    empty_table = pa.Table.from_pydict({})
                    sink = io.BytesIO()
                    with pa.ipc.new_stream(sink, empty_table.schema) as writer:
                        writer.write_table(empty_table)
                    return Response(
                        content=sink.getvalue(),
                        media_type="application/vnd.apache.arrow.stream",
                    )

            spoken_dir = os.path.join(DATA_ROOT, "parquet", "spoken_language")
            datasets = [
                d
                for d in os.listdir(spoken_dir)
                if os.path.isdir(os.path.join(spoken_dir, d))
            ]

            # Optimization: only query datasets that have the glosses
            gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
            try:
                matching_datasets = (
                    con.execute(
                        f"SELECT DISTINCT dataset_name FROM read_parquet('{GLOSS_INDEX_PARQUET}') WHERE Concepticon_Gloss IN ('{gloss_list}')"
                    )
                    .df()["dataset_name"]
                    .tolist()
                )
                datasets = [d for d in datasets if d in set(matching_datasets)]
            except:
                pass

            subqueries = []
            for d in datasets:
                ds_path = os.path.join(spoken_dir, d)
                l_p = os.path.join(ds_path, "languages.parquet").replace("\\", "/")
                f_p = os.path.join(ds_path, "forms.parquet").replace("\\", "/")
                p_p = os.path.join(ds_path, "parameters.parquet").replace("\\", "/")

                if all(os.path.exists(p) for p in [l_p, f_p, p_p]):
                    subqueries.append(
                        f"""
                        SELECT l.*, f.Value as form_value, p.Concepticon_Gloss as parameter_name, '{d}' as dataset_name
                        FROM read_parquet('{p_p}') p
                        JOIN read_parquet('{f_p}') f ON p.ID = f.Parameter_ID
                        JOIN read_parquet('{l_p}') l ON f.Language_ID = l.ID
                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {get_coordinate_filter_sql('l')}
                    """
                    )

            if not subqueries:
                if format == "json":
                    return {"data": [], "total": 0}
                else:
                    return Response(status_code=204)

            source_query = "(" + " UNION ALL ".join(subqueries) + ")"

            conds = ["1=1"]
            if search:
                conds.append(
                    f"(LOWER(Name) LIKE LOWER('%{search}%') OR LOWER(dataset_name) LIKE LOWER('%{search}%'))"
                )
            if form_filter:
                conds.append(f"LOWER(form_value) LIKE LOWER('%{form_filter}%')")
            if parameter_filter:
                conds.append(
                    f"LOWER(parameter_name) LIKE LOWER('%{parameter_filter}%')"
                )

            where = " AND ".join(conds)
            query = f"SELECT * FROM {source_query} WHERE {where} LIMIT {limit} OFFSET {offset}"
            count_query = f"SELECT count(*) FROM {source_query} WHERE {where}"
        else:
            dataset_path = os.path.join(DATA_ROOT, "parquet", data_type, dataset)
            if not os.path.exists(dataset_path):
                raise HTTPException(status_code=404, detail="Dataset not found")

            source_query = ""
            # Construct Query
            if data_type in ["spoken_language", "sign_language"]:
                lang_parquet = os.path.join(dataset_path, "languages.parquet").replace(
                    "\\", "/"
                )
                forms_parquet = os.path.join(dataset_path, "forms.parquet").replace(
                    "\\", "/"
                )
                params_parquet = os.path.join(
                    dataset_path, "parameters.parquet"
                ).replace("\\", "/")

                coord_where = get_coordinate_filter_sql("l")

                if glosses:
                    gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
                    source_query = f"""
                    (
                        SELECT l.*, f.Value as form_value, p.Concepticon_Gloss as parameter_name
                        FROM read_parquet('{params_parquet}') p
                        JOIN read_parquet('{forms_parquet}') f ON p.ID = f.Parameter_ID
                        JOIN read_parquet('{lang_parquet}') l ON f.Language_ID = l.ID
                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {coord_where}
                    )
                    """
                elif form_filter or parameter_filter:
                    source_query = f"""
                    (
                        SELECT l.*, f.Value as form_value, p.Concepticon_Gloss as parameter_name
                        FROM read_parquet('{lang_parquet}') l
                        JOIN read_parquet('{forms_parquet}') f ON l.ID = f.Language_ID
                        JOIN read_parquet('{params_parquet}') p ON f.Parameter_ID = p.ID
                        WHERE {coord_where}
                    )
                    """
                else:
                    source_query = f"(SELECT * FROM read_parquet('{lang_parquet}') WHERE {get_coordinate_filter_sql()})"

                conds = ["1=1"]
                if search:
                    conds.append(f"LOWER(Name) LIKE LOWER('%{search}%')")
                if form_filter:
                    conds.append(f"LOWER(form_value) LIKE LOWER('%{form_filter}%')")
                if parameter_filter:
                    conds.append(
                        f"LOWER(parameter_name) LIKE LOWER('%{parameter_filter}%')"
                    )
                where = " AND ".join(conds)
                query = f"SELECT * FROM {source_query} WHERE {where} LIMIT {limit} OFFSET {offset}"
                count_query = f"SELECT count(*) FROM {source_query} WHERE {where}"
            else:
                parquet_file = os.path.join(
                    dataset_path, f"{data_type}.parquet"
                ).replace("\\", "/")
                coord_where = get_coordinate_filter_sql()
                conds = [coord_where]
                if search:
                    conds.append(
                        f"(LOWER(Name) LIKE LOWER('%{search}%') OR LOWER(Description) LIKE LOWER('%{search}%'))"
                    )
                where = " AND ".join(conds)
                query = f"SELECT * FROM read_parquet('{parquet_file}') WHERE {where} LIMIT {limit} OFFSET {offset}"
                count_query = (
                    f"SELECT count(*) FROM read_parquet('{parquet_file}') WHERE {where}"
                )

        # Execution based on format
        if format == "json":
            df = con.execute(query).df()
            results = sanitize_df(df)
            total = con.execute(count_query).fetchone()[0]
            return {"data": results, "total": total}
        else:
            # Default: Arrow IPC Stream
            arrow_table = con.execute(query).fetch_arrow_table()

            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)

            return Response(
                content=sink.getvalue(),
                media_type="application/vnd.apache.arrow.stream",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@router.get("/full_data")
async def get_full_data(
    data_type: str,
    dataset: str,
    glosses: Optional[List[str]] = Query(None),
    format: Optional[str] = Query(None),
):
    con = get_db_connection()
    try:
        if dataset.startswith(COMBINED_DATASET_NAME):
            if not glosses:
                if format == "json":
                    return {"data": []}
                else:
                    empty_table = pa.Table.from_pydict({})
                    sink = io.BytesIO()
                    with pa.ipc.new_stream(sink, empty_table.schema) as writer:
                        writer.write_table(empty_table)
                    return Response(
                        content=sink.getvalue(),
                        media_type="application/vnd.apache.arrow.stream",
                    )

            spoken_dir = os.path.join(DATA_ROOT, "parquet", "spoken_language")
            datasets = [
                d
                for d in os.listdir(spoken_dir)
                if os.path.isdir(os.path.join(spoken_dir, d))
            ]

            gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
            try:
                matching_datasets = (
                    con.execute(
                        f"SELECT DISTINCT dataset_name FROM read_parquet('{GLOSS_INDEX_PARQUET}') WHERE Concepticon_Gloss IN ('{gloss_list}')"
                    )
                    .df()["dataset_name"]
                    .tolist()
                )
                datasets = [d for d in datasets if d in set(matching_datasets)]
            except:
                pass

            subqueries = []
            for d in datasets:
                ds_path = os.path.join(spoken_dir, d)
                l_p = os.path.join(ds_path, "languages.parquet").replace("\\", "/")
                f_p = os.path.join(ds_path, "forms.parquet").replace("\\", "/")
                p_p = os.path.join(ds_path, "parameters.parquet").replace("\\", "/")
                if all(os.path.exists(p) for p in [l_p, f_p, p_p]):
                    subqueries.append(
                        f"""
                        SELECT l.ID, l.Glottocode, CAST(l.Latitude AS DOUBLE) as Latitude, CAST(l.Longitude AS DOUBLE) as Longitude, f.Value as form_value, p.Concepticon_Gloss as parameter_name, '{d}' as dataset_name
                        FROM read_parquet('{p_p}') p
                        JOIN read_parquet('{f_p}') f ON p.ID = f.Parameter_ID
                        JOIN read_parquet('{l_p}') l ON f.Language_ID = l.ID
                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {get_coordinate_filter_sql('l')}
                    """
                    )

            if not subqueries:
                if format == "json":
                    return {"data": []}
                else:
                    return Response(status_code=204)

            source_query = "(" + " UNION ALL ".join(subqueries) + ")"
        else:
            dataset_path = os.path.join(DATA_ROOT, "parquet", data_type, dataset)
            if not os.path.exists(dataset_path):
                raise HTTPException(status_code=404, detail="Dataset not found")

            source_query = ""
            if data_type in ["spoken_language", "sign_language"]:
                lang_parquet = os.path.join(dataset_path, "languages.parquet").replace(
                    "\\", "/"
                )
                coord_where = get_coordinate_filter_sql("l")
                if glosses:
                    forms_parquet = os.path.join(dataset_path, "forms.parquet").replace(
                        "\\", "/"
                    )
                    params_parquet = os.path.join(
                        dataset_path, "parameters.parquet"
                    ).replace("\\", "/")
                    gloss_list = "', '".join([g.replace("'", "''") for g in glosses])
                    source_query = f"""
                    (
                        SELECT l.ID, l.Glottocode, CAST(l.Latitude AS DOUBLE) as Latitude, CAST(l.Longitude AS DOUBLE) as Longitude, f.Value as form_value, p.Concepticon_Gloss as parameter_name
                        FROM read_parquet('{params_parquet}') p
                        JOIN read_parquet('{forms_parquet}') f ON p.ID = f.Parameter_ID
                        JOIN read_parquet('{lang_parquet}') l ON f.Language_ID = l.ID
                        WHERE p.Concepticon_Gloss IN ('{gloss_list}') AND {coord_where}
                    )
                    """
                else:
                    source_query = f"(SELECT ID, Glottocode, CAST(Latitude AS DOUBLE) as Latitude, CAST(Longitude AS DOUBLE) as Longitude FROM read_parquet('{lang_parquet}') WHERE {get_coordinate_filter_sql()})"
            else:
                parquet_file = os.path.join(
                    dataset_path, f"{data_type}.parquet"
                ).replace("\\", "/")
                source_query = f"(SELECT * FROM read_parquet('{parquet_file}') WHERE {get_coordinate_filter_sql()})"

        query = f"SELECT * FROM {source_query}"

        if format == "json":
            df = con.execute(query).df()
            results = sanitize_df(df)
            return {"data": results}
        else:
            arrow_table = con.execute(query).fetch_arrow_table()
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
                writer.write_table(arrow_table)

            return Response(
                content=sink.getvalue(),
                media_type="application/vnd.apache.arrow.stream",
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()
